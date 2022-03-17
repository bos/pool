-- | Internal implementation details for "Data.Pool".
--
-- This module is intended for internal use only, and may change without warning
-- in subsequent releases.
{-# OPTIONS_HADDOCK hide, not-home #-}
module Data.Pool.Internal where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.IORef
import Data.Primitive.SmallArray
import GHC.Clock
import qualified Data.List as L

-- | Striped resource pool based on "Control.Concurrent.QSem".
--
-- The number of stripes is arranged to be equal to the number of capabilities
-- so that they never compete over access to the same stripe. This results in a
-- very good performance in a multi-threaded environment.
data Pool a = Pool
  { createResource :: !(IO a)
  , freeResource   :: !(a -> IO ())
  , localPools     :: !(SmallArray (LocalPool a))
  , reaperRef      :: !(IORef ())
  }

-- | A single, capability-local pool.
data LocalPool a = LocalPool !Int !(MVar (Stripe a))

-- | Stripe of a resource pool. If @available@ is 0, the list of threads waiting
-- for a resource (each with an associated 'MVar') is @queue ++ reverse queueR@.
data Stripe a = Stripe
  { available :: !Int
  , cache     :: ![Entry a]
  , queue     :: !(Queue a)
  , queueR    :: !(Queue a)
  }

-- | An existing resource currently sitting in a pool.
data Entry a = Entry
  { entry    :: a
  , lastUsed :: !Double
  }

-- | A queue of MVarS corresponding to threads waiting for resources.
--
-- Basically a monomorphic list to save two pointer indirections.
data Queue a = Queue !(MVar (Maybe a)) (Queue a) | Empty

-- | Create a new striped resource pool.
--
-- The number of stripes is equal to the number of capabilities @N@.
--
-- /Note:/ although the garbage collector will destroy all idle resources when
-- the pool is garbage collected, it's recommended to manually call
-- 'destroyAllResources' when you're done with the pool so that the resources
-- are freed up as soon as possible.
newPool
  :: IO a
  -- ^ The action that creates a new resource.
  -> (a -> IO ())
  -- ^ The action that destroys an existing resource.
  -> Double
  -- ^ The amount of seconds for which an unused resource is kept open. The
  -- smallest acceptable value is @0.5@.
  --
  -- /Note:/ the elapsed time before destroying a resource may be a little
  -- longer than requested, as the collector thread wakes at 1-second intervals.
  -> Int
  -- ^ The number of resources to keep open across all stripes. The smallest
  -- acceptable value is @1@.
  --
  -- /Note:/ for each stripe the number of resources is divided by the number of
  -- capabilities and rounded up. Therefore the pool might end up creating up to
  -- @N - 1@ resources more in total than specified.
  -> IO (Pool a)
newPool create free idleTime maxResources = do
  when (idleTime < 0.5) $ do
    error "idleTime must be at least 0.5"
  when (maxResources < 1) $ do
    error "maxResources must be at least 1"
  numStripes <- getNumCapabilities
  when (numStripes < 1) $ do
    error "numStripes must be at least 1"
  pools <- fmap (smallArrayFromListN numStripes) . forM [1..numStripes] $ \n -> do
    LocalPool n <$> newMVar Stripe
      { available = maxResources `quotCeil` numStripes
      , cache     = []
      , queue     = Empty
      , queueR    = Empty
      }
  mask_ $ do
    ref        <- newIORef ()
    collectorA <- forkIOWithUnmask $ \unmask -> unmask $ collector pools
    void . mkWeakIORef ref $ do
      killThread collectorA
      cleanLocalPools (const True) free pools
    pure Pool { createResource = create
              , freeResource   = free
              , localPools     = pools
              , reaperRef      = ref
              }
  where
    quotCeil :: Int -> Int -> Int
    quotCeil x y =
      -- Basically ceiling (x / y) without going through Double.
      let (z, r) = x `quotRem` y in if r == 0 then z else z + 1

    -- Collect stale resources from the pool once per second.
    collector pools = forever $ do
      threadDelay 1000000
      now <- getMonotonicTime
      let isStale e = now - lastUsed e > idleTime
      cleanLocalPools isStale free pools

-- | Destroy a resource.
--
-- Note that this will ignore any exceptions in the destroy function.
destroyResource :: Pool a -> LocalPool a -> a -> IO ()
destroyResource pool (LocalPool _ mstripe) a = do
  uninterruptibleMask_ $ do -- Note [signal uninterruptible]
    stripe <- takeMVar mstripe
    newStripe <- signal stripe Nothing
    putMVar mstripe newStripe
    void . try @SomeException $ freeResource pool a

-- | Return a resource to the given 'LocalPool'.
putResource :: LocalPool a -> a -> IO ()
putResource (LocalPool _ mstripe) a = do
  uninterruptibleMask_ $ do -- Note [signal uninterruptible]
    stripe    <- takeMVar mstripe
    newStripe <- signal stripe (Just a)
    putMVar mstripe newStripe

-- | Destroy all resources in all stripes in the pool.
--
-- Note that this will ignore any exceptions in the destroy function.
--
-- This function is useful when you detect that all resources in the pool are
-- broken. For example after a database has been restarted all connections
-- opened before the restart will be broken. In that case it's better to close
-- those connections so that 'takeResource' won't take a broken connection from
-- the pool but will open a new connection instead.
--
-- Another use-case for this function is that when you know you are done with
-- the pool you can destroy all idle resources immediately instead of waiting on
-- the garbage collector to destroy them, thus freeing up those resources
-- sooner.
destroyAllResources :: Pool a -> IO ()
destroyAllResources pool = do
  cleanLocalPools (const True) (freeResource pool) (localPools pool)

----------------------------------------
-- Helpers

-- | Get a capability-local pool.
getLocalPool :: SmallArray (LocalPool a) -> IO (LocalPool a)
getLocalPool pools = do
  (cid, _) <- threadCapability =<< myThreadId
  pure $ pools `indexSmallArray` (cid `rem` sizeofSmallArray pools)

-- | Wait for the resource to be put into a given 'MVar'.
waitForResource :: MVar (Stripe a) -> MVar (Maybe a) -> IO (Maybe a)
waitForResource mstripe q = takeMVar q `onException` cleanup
  where
    cleanup = uninterruptibleMask_ $ do -- Note [signal uninterruptible]
      stripe    <- takeMVar mstripe
      newStripe <- tryTakeMVar q >>= \case
        Just ma -> do
          -- Between entering the exception handler and taking ownership of
          -- the stripe we got the resource we wanted. We don't need it
          -- anymore though, so pass it to someone else.
          signal stripe ma
        Nothing -> do
          -- If we're still waiting, fill up the MVar with an undefined value
          -- so that 'signal' can discard our MVar from the queue.
          putMVar q $ error "unreachable"
          pure stripe
      putMVar mstripe newStripe

-- | If an exception is received while a resource is being created, restore the
-- original size of the stripe.
restoreSize :: MVar (Stripe a) -> IO ()
restoreSize mstripe = uninterruptibleMask_ $ do
  -- 'uninterruptibleMask_' is used since 'takeMVar' might block.
  stripe <- takeMVar mstripe
  putMVar mstripe $! stripe { available = available stripe + 1 }

-- | Free resource entries in the stripes that fulfil a given condition.
cleanLocalPools
  :: (Entry a -> Bool)
  -> (a -> IO ())
  -> SmallArray (LocalPool a)
  -> IO ()
cleanLocalPools isStale free pools = do
  mask $ \unmask -> forM_ pools $ \(LocalPool _ mstripe) -> do
    -- Asynchronous exceptions need to be masked here to prevent leaking of
    -- 'stale' resources before they're freed.
    stale <- modifyMVar mstripe $ \stripe -> unmask $ do
      let (stale, fresh) = L.partition isStale (cache stripe)
          -- There's no need to update 'available' here because it only tracks
          -- the number of resources taken from the pool.
          newStripe      = stripe { cache = fresh }
      newStripe `seq` pure (newStripe, map entry stale)
    -- We need to ignore exceptions in the 'free' function, otherwise if an
    -- exception is thrown half-way, we leak the rest of the resources. Also,
    -- asynchronous exceptions need to be hard masked here since freeing a
    -- resource might in theory block.
    uninterruptibleMask_ . forM_ stale $ try @SomeException . free

-- Note [signal uninterruptible]
--
--   If we have
--
--      bracket takeResource putResource (...)
--
--   and an exception arrives at the putResource, then we must not lose the
--   resource. The putResource is masked by bracket, but taking the MVar might
--   block, and so it would be interruptible. Hence we need an uninterruptible
--   variant of mask here.
signal :: Stripe a -> Maybe a -> IO (Stripe a)
signal stripe ma = if available stripe == 0
  then loop (queue stripe) (queueR stripe)
  else do
    newCache <- case ma of
      Just a -> do
        now <- getMonotonicTime
        pure $ Entry a now : cache stripe
      Nothing -> pure $ cache stripe
    pure $! stripe { available = available stripe + 1
                   , cache = newCache
                   }
  where
    loop Empty Empty = do
      newCache <- case ma of
        Just a -> do
          now <- getMonotonicTime
          pure [Entry a now]
        Nothing -> pure []
      pure $! Stripe { available = 1
                     , cache = newCache
                     , queue = Empty
                     , queueR = Empty
                     }
    loop Empty        qR = loop (reverseQueue qR) Empty
    loop (Queue q qs) qR = tryPutMVar q ma >>= \case
      -- This fails when 'waitForResource' went into the exception handler and
      -- filled the MVar (with an undefined value) itself. In such case we
      -- simply ignore it.
      False -> loop qs qR
      True  -> pure $! stripe { available = 0
                              , queue = qs
                              , queueR = qR
                              }

reverseQueue :: Queue a -> Queue a
reverseQueue = go Empty
  where
    go acc = \case
      Empty      -> acc
      Queue x xs -> go (Queue x acc) xs
