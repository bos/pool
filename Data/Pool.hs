{-# LANGUAGE CPP, NamedFieldPuns, RecordWildCards, ScopedTypeVariables #-}

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
-- Maintainer:  Bryan O'Sullivan <bos@serpentine.com>
-- Stability:   experimental
-- Portability: portable
--
-- A high-performance striped pooling abstraction for managing
-- flexibly-sized collections of resources such as database
-- connections.
--
-- \"Striped\" means that a single 'Pool' consists of several
-- sub-pools, each managed independently.  A stripe size of 1 is fine
-- for many applications, and probably what you should choose by
-- default.  Larger stripe sizes will lead to reduced contention in
-- high-performance multicore applications, at a trade-off of causing
-- the maximum number of simultaneous resources in use to grow.
module Data.Pool
    (
      Pool(idleTime, maxResources, numStripes)
    , LocalPool
    , createPool
    , withResource
    , takeResource
    , destroyResource
    , putResource
    ) where

import Control.Applicative ((<$>))
import Control.Concurrent (forkIO, killThread, myThreadId, threadDelay)
import Control.Concurrent.STM
import Control.Exception (SomeException, catch, onException)
import Control.Monad (forM_, forever, join, liftM2, unless, when)
import Data.Hashable (hash)
import Data.List (partition)
import Data.Time.Clock (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Prelude hiding (catch)
import System.Mem.Weak (addFinalizer)
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
    }

data Pool a = Pool {
      create :: IO a
    -- ^ Action for creating a new entry to add to the pool.
    , destroy :: a -> IO ()
    -- ^ Action for destroying an entry that is now done with.
    , numStripes :: Int
    -- ^ Stripe count.  The number of distinct sub-pools to maintain.
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
    }

instance Show (Pool a) where
    show Pool{..} = "Pool {numStripes = " ++ show numStripes ++ ", " ++
                    "idleTime = " ++ show idleTime ++ ", " ++
                    "maxResources = " ++ show maxResources ++ "}"

createPool
    :: IO a
    -- ^ Action that creates a new resource.
    -> (a -> IO ())
    -- ^ Action that destroys an existing resource.
    -> Int
    -- ^ Stripe count.  The number of distinct sub-pools to maintain.
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
  localPools <- atomically . V.replicateM numStripes $
                liftM2 LocalPool (newTVar 0) (newTVar [])
  reaperId <- forkIO $ reaper destroy idleTime localPools
  let p = Pool {
            create
          , destroy
          , numStripes
          , idleTime
          , maxResources
          , localPools
          }
  addFinalizer p $ killThread reaperId
  return p

-- | Periodically go through all pools, closing any resources that
-- have been left idle for too long.
reaper :: (a -> IO ()) -> NominalDiffTime -> V.Vector (LocalPool a) -> IO ()
reaper destroy idleTime pools = forever $ do
  threadDelay (1 * 1000000)
  now <- getCurrentTime
  let isStale Entry{..} = now `diffUTCTime` lastUse > idleTime
  V.forM_ pools $ \LocalPool{..} -> do
    resources <- atomically $ do
      (stale,fresh) <- partition isStale <$> readTVar entries
      unless (null stale) $ do
        writeTVar entries fresh
        modifyTVar_ inUse (subtract (length stale))
      return (map entry stale)
    forM_ resources $ \resource -> do
      destroy resource `catch` \(_::SomeException) -> return ()
              
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
takeResource Pool{..} = do
  i <- liftBase $ ((`mod` numStripes) . hash) <$> myThreadId
  let pool@LocalPool{..} = localPools V.! i
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
  return (resource, pool)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE takeResource #-}
#endif

-- | Destroy a resource. Note that this will ignore any exceptions in the
-- destroy function.
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource Pool{..} LocalPool{..} resource = do
   destroy resource `catch` \(_::SomeException) -> return ()
   atomically (modifyTVar_ inUse (subtract 1))
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE destroyResource #-}
#endif

-- | Return a resource to the given 'LocalPool'.
putResource :: LocalPool a -> a -> IO ()
putResource LocalPool{..} resource = do
    now <- getCurrentTime
    atomically $ modifyTVar_ entries (Entry resource now:)
#if __GLASGOW_HASKELL__ >= 700
{-# INLINABLE putResource #-}
#endif

modifyTVar_ :: TVar a -> (a -> a) -> STM ()
modifyTVar_ v f = readTVar v >>= \a -> writeTVar v $! f a

modError :: String -> String -> a
modError func msg =
    error $ "Data.Pool." ++ func ++ ": " ++ msg
