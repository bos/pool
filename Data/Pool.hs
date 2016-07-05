{-# LANGUAGE CPP, NamedFieldPuns, RecordWildCards, ScopedTypeVariables, RankNTypes, DeriveDataTypeable #-}

#if MIN_VERSION_monad_control(0,3,0)
{-# LANGUAGE FlexibleContexts #-}
#endif

#if !MIN_VERSION_base(4,3,0)
{-# LANGUAGE RankNTypes #-}
#endif

-- |
-- Module:      Data.Pool
-- Copyright:   (c) 2011 MailRank, Inc.
-- License:     BSD3
-- Maintainer:  Bryan O'Sullivan <bos@serpentine.com>,
--              Bas van Dijk <v.dijk.bas@gmail.com>
-- Stability:   experimental
-- Portability: portable
--
-- A high-performance striped pooling abstraction for managing
-- flexibly-sized collections of resources such as database
-- connections.
--
-- \"Striped\" means that a single 'Pool' consists of several
-- sub-pools, each managed independently.  A single stripe is fine for
-- many applications, and probably what you should choose by default.
-- More stripes will lead to reduced contention in high-performance
-- multicore applications, at a trade-off of causing the maximum
-- number of simultaneous resources in use to grow.
module Data.Pool
    (
      Pool(idleTime, maxResources, numStripes)
    , LocalPool
    , createPool
    , withResource
    , takeResource
    , tryWithResource
    , tryTakeResource
    , destroyResource
    , putResource
    , destroyAllResources
    ) where

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent (myThreadId)
import Control.Concurrent.STM
import Control.Exception (SomeException, onException)
import Control.Monad (forM_, join, unless, when)
import Control.Concurrent.AlarmClock
import Data.Hashable (hash)
import Data.IORef (IORef, newIORef, mkWeakIORef)
import Data.List (partition)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime, addUTCTime)
import Data.Typeable (Typeable)
import qualified Control.Exception as E
import qualified Data.Vector as V

#if MIN_VERSION_monad_control(0,3,0)
import Control.Monad.Trans.Control (MonadBaseControl, control)
import Control.Monad.Base (liftBase)
#else
import Control.Monad.IO.Control (MonadControlIO, controlIO)
import Control.Monad.IO.Class (liftIO)
#define control controlIO
#define liftBase liftIO
#endif

#if MIN_VERSION_base(4,3,0)
import Control.Exception (mask)
#else
-- Don't do any async exception protection for older GHCs.
mask :: ((forall a. IO a -> IO a) -> IO b) -> IO b
mask f = f id
#endif

-- | A single resource pool entry.
data Entry a = Entry {
      entry :: a
    , lastUse :: UTCTime
    -- ^ Time of last return.
    }

-- | A single striped pool.
data LocalPool a = LocalPool {
      inUse :: TVar Int
    -- ^ Count of open entries (both idle and in use).
    , entries :: TVar [Entry a]
    -- ^ Idle entries.
    , setLastUsedTime :: UTCTime -> STM ()
    -- ^ Record the last-used time of a resource, to request a reaper wakeup.
    , lfin :: IORef ()
    -- ^ empty value used to attach a finalizer to (internal)
    } deriving (Typeable)

data Pool a = Pool {
      create :: IO a
    -- ^ Action for creating a new entry to add to the pool.
    , destroy :: a -> IO ()
    -- ^ Action for destroying an entry that is now done with.
    , numStripes :: Int
    -- ^ The number of stripes (distinct sub-pools) to maintain.
    -- The smallest acceptable value is 1.
    , idleTime :: NominalDiffTime
    -- ^ Amount of time for which an unused resource is kept alive.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before closing may be a little longer than
    -- requested, as the reaper thread wakes at 1-second intervals.
    , maxResources :: Int
    -- ^ Maximum number of resources to maintain per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
    , localPools :: V.Vector (LocalPool a)
    -- ^ Per-capability resource pools.
    , fin :: IORef ()
    -- ^ empty value used to attach a finalizer to (internal)
    } deriving (Typeable)

instance Show (Pool a) where
    show Pool{..} = "Pool {numStripes = " ++ show numStripes ++ ", " ++
                    "idleTime = " ++ show idleTime ++ ", " ++
                    "maxResources = " ++ show maxResources ++ "}"

-- | Create a striped resource pool.
--
-- Although the garbage collector will destroy all idle resources when
-- the pool is garbage collected it's recommended to manually
-- 'destroyAllResources' when you're done with the pool so that the
-- resources are freed up as soon as possible.
createPool
    :: IO a
    -- ^ Action that creates a new resource.
    -> (a -> IO ())
    -- ^ Action that destroys an existing resource.
    -> Int
    -- ^ The number of stripes (distinct sub-pools) to maintain.
    -- The smallest acceptable value is 1.
    -> NominalDiffTime
    -- ^ Amount of time for which an unused resource is kept open.
    -- The smallest acceptable value is 0.5 seconds.
    --
    -- The elapsed time before destroying a resource may be a little
    -- longer than requested, as the reaper thread wakes at 1-second
    -- intervals.
    -> Int
    -- ^ Maximum number of resources to keep open per stripe.  The
    -- smallest acceptable value is 1.
    --
    -- Requests for resources will block if this limit is reached on a
    -- single stripe, even if other stripes have idle resources
    -- available.
     -> IO (Pool a)
createPool create destroy numStripes idleTime maxResources = do
  when (numStripes < 1) $
    modError "pool " $ "invalid stripe count " ++ show numStripes
  when (idleTime < 0.5) $
    modError "pool " $ "invalid idle time " ++ show idleTime
  when (maxResources < 1) $
    modError "pool " $ "invalid maximum resource count " ++ show maxResources

  localPoolStates <- V.replicateM numStripes $ (,) <$> newTVarIO 0 <*> newTVarIO []
  alarmClock <- newAlarmClock $ reapStaleEntries destroy idleTime localPoolStates

  let handleSetLastUsedTime t = do
        isSet <- isAlarmSetSTM alarmClock
        unless isSet $ setAlarmSTM alarmClock $ addUTCTime idleTime t

  localPools <- V.forM localPoolStates $ \(inUse, entries) ->
    LocalPool inUse entries handleSetLastUsedTime <$> newIORef ()

  fin <- newIORef ()
  let p = Pool {
            create
          , destroy
          , numStripes
          , idleTime
          , maxResources
          , localPools
          , fin
          }
  mkWeakIORef fin (destroyAlarmClock alarmClock) >>
    V.mapM_ (\lp -> mkWeakIORef (lfin lp) (purgeLocalPool destroy lp)) localPools
  return p

-- | Periodically go through all pools, closing any resources that
-- have been left idle for too long.
reapStaleEntries :: (a -> IO ()) -> NominalDiffTime -> V.Vector (TVar Int, TVar [Entry a]) -> AlarmClock UTCTime -> IO ()
reapStaleEntries destroy idleTime poolStates alarmClock = do
  now <- getCurrentTime
  let isStale Entry{..} = now `diffUTCTime` lastUse > idleTime
  V.forM_ poolStates $ \(inUse, entries) -> do
    resources <- atomically $ do
      (stale,fresh) <- partition isStale <$> readTVar entries
      unless (null stale) $ do
        writeTVar entries fresh
        modifyTVar_ inUse (subtract (length stale))
      unless (null fresh) $
        setAlarmSTM alarmClock $ addUTCTime idleTime $ minimum $ map lastUse fresh
      return (map entry stale)
    forM_ resources $ \resource -> do
      destroy resource `E.catch` \(_::SomeException) -> return ()

-- | Destroy all idle resources of the given 'LocalPool' and remove them from
-- the pool.
purgeLocalPool :: (a -> IO ()) -> LocalPool a -> IO ()
purgeLocalPool destroy LocalPool{..} = do
  resources <- atomically $ do
    idle <- swapTVar entries []
    modifyTVar_ inUse (subtract (length idle))
    return (map entry idle)
  forM_ resources $ \resource ->
    destroy resource `E.catch` \(_::SomeException) -> return ()

-- | Temporarily take a resource from a 'Pool', perform an action with
-- it, and return it to the pool afterwards.
--
-- * If the pool has an idle resource available, it is used
--   immediately.
--
-- * Otherwise, if the maximum number of resources has not yet been
--   reached, a new resource is created and used.
--
-- * If the maximum number of resources has been reached, this
--   function blocks until a resource becomes available.
--
-- If the action throws an exception of any type, the resource is
-- destroyed, and not returned to the pool.
--
-- It probably goes without saying that you should never manually
-- destroy a pooled resource, as doing so will almost certainly cause
-- a subsequent user (who expects the resource to be valid) to throw
-- an exception.
withResource ::
#if MIN_VERSION_monad_control(0,3,0)
    (MonadBaseControl IO m)
#else
    (MonadControlIO m)
#endif
  => Pool a -> (a -> m b) -> m b
{-# SPECIALIZE withResource :: Pool a -> (a -> IO b) -> IO b #-}
withResource pool act = control $ \runInIO -> mask $ \restore -> do
  (resource, local) <- takeResource pool
  ret <- restore (runInIO (act resource)) `onException`
            destroyResource pool local resource
  putResource local resource
  return ret
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE withResource #-}
#endif

-- | Take a resource from the pool, following the same results as
-- 'withResource'. Note that this function should be used with caution, as
-- improper exception handling can lead to leaked resources.
--
-- This function returns both a resource and the @LocalPool@ it came from so
-- that it may either be destroyed (via 'destroyResource') or returned to the
-- pool (via 'putResource').
takeResource :: Pool a -> IO (a, LocalPool a)
takeResource pool@Pool{..} = do
  local@LocalPool{..} <- getLocalPool pool
  resource <- liftBase . join . atomically $ do
    ents <- readTVar entries
    case ents of
      (Entry{..}:es) -> writeTVar entries es >> return (return entry)
      [] -> do
        used <- readTVar inUse
        when (used == maxResources) retry
        writeTVar inUse $! used + 1
        return $
          create `onException` atomically (modifyTVar_ inUse (subtract 1))
  return (resource, local)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE takeResource #-}
#endif

-- | Similar to 'withResource', but only performs the action if a resource could
-- be taken from the pool /without blocking/. Otherwise, 'tryWithResource'
-- returns immediately with 'Nothing' (ie. the action function is /not/ called).
-- Conversely, if a resource can be borrowed from the pool without blocking, the
-- action is performed and it's result is returned, wrapped in a 'Just'.
tryWithResource :: forall m a b.
#if MIN_VERSION_monad_control(0,3,0)
    (MonadBaseControl IO m)
#else
    (MonadControlIO m)
#endif
  => Pool a -> (a -> m b) -> m (Maybe b)
tryWithResource pool act = control $ \runInIO -> mask $ \restore -> do
  res <- tryTakeResource pool
  case res of
    Just (resource, local) -> do
      ret <- restore (runInIO (Just <$> act resource)) `onException`
                destroyResource pool local resource
      putResource local resource
      return ret
    Nothing -> restore . runInIO $ return (Nothing :: Maybe b)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE tryWithResource #-}
#endif

-- | A non-blocking version of 'takeResource'. The 'tryTakeResource' function
-- returns immediately, with 'Nothing' if the pool is exhausted, or @'Just' (a,
-- 'LocalPool' a)@ if a resource could be borrowed from the pool successfully.
tryTakeResource :: Pool a -> IO (Maybe (a, LocalPool a))
tryTakeResource pool@Pool{..} = do
  local@LocalPool{..} <- getLocalPool pool
  resource <- liftBase . join . atomically $ do
    ents <- readTVar entries
    case ents of
      (Entry{..}:es) -> writeTVar entries es >> return (return . Just $ entry)
      [] -> do
        used <- readTVar inUse
        if used == maxResources
          then return (return Nothing)
          else do
            writeTVar inUse $! used + 1
            return $ Just <$>
              create `onException` atomically (modifyTVar_ inUse (subtract 1))
  return $ (flip (,) local) <$> resource
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE tryTakeResource #-}
#endif

-- | Get a (Thread-)'LocalPool'
--
-- Internal, just to not repeat code for 'takeResource' and 'tryTakeResource'
getLocalPool :: Pool a -> IO (LocalPool a)
getLocalPool Pool{..} = do
  i <- liftBase $ ((`mod` numStripes) . hash) <$> myThreadId
  return $ localPools V.! i
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE getLocalPool #-}
#endif

-- | Destroy a resource. Note that this will ignore any exceptions in the
-- destroy function.
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource Pool{..} LocalPool{..} resource = do
   destroy resource `E.catch` \(_::SomeException) -> return ()
   atomically (modifyTVar_ inUse (subtract 1))
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE destroyResource #-}
#endif

-- | Return a resource to the given 'LocalPool'.
putResource :: LocalPool a -> a -> IO ()
putResource LocalPool{..} resource = do
    now <- getCurrentTime
    atomically $ do
      modifyTVar_ entries (Entry resource now:)
      setLastUsedTime now
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE putResource #-}
#endif

-- | Destroy all resources in all stripes in the pool. Note that this
-- will ignore any exceptions in the destroy function.
--
-- This function is useful when you detect that all resources in the
-- pool are broken. For example after a database has been restarted
-- all connections opened before the restart will be broken. In that
-- case it's better to close those connections so that 'takeResource'
-- won't take a broken connection from the pool but will open a new
-- connection instead.
--
-- Another use-case for this function is that when you know you are
-- done with the pool you can destroy all idle resources immediately
-- instead of waiting on the garbage collector to destroy them, thus
-- freeing up those resources sooner.
destroyAllResources :: Pool a -> IO ()
destroyAllResources Pool{..} = V.forM_ localPools $ purgeLocalPool destroy

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a

modError :: String -> String -> a
modError func msg =
    error $ "Data.Pool." ++ func ++ ": " ++ msg
