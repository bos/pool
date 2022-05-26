-- | A high-performance pooling abstraction for managing flexibly-sized
-- collections of resources such as database connections.
module Data.Pool
  ( -- * Pool
    Pool
  , LocalPool
  , newPool

    -- * Resource management
  , withResource
  , takeResource
  , putResource
  , destroyResource
  , destroyAllResources

    -- * Compatibility with 0.2
  , createPool
  ) where

import Control.Concurrent
import Control.Exception
import Data.Time (NominalDiffTime)

import Data.Pool.Internal

-- | Take a resource from the pool, perform an action with it and return it to
-- the pool afterwards.
--
-- * If the pool has an idle resource available, it is used immediately.
--
-- * Otherwise, if the maximum number of resources has not yet been reached, a
--   new resource is created and used.
--
-- * If the maximum number of resources has been reached, this function blocks
--   until a resource becomes available.
--
-- If the action throws an exception of any type, the resource is destroyed and
-- not returned to the pool.
--
-- It probably goes without saying that you should never manually destroy a
-- pooled resource, as doing so will almost certainly cause a subsequent user
-- (who expects the resource to be valid) to throw an exception.
withResource :: Pool a -> (a -> IO r) -> IO r
withResource pool act = mask $ \unmask -> do
  (res, localPool) <- takeResource pool
  r                <- unmask (act res) `onException` destroyResource pool localPool res
  putResource localPool res
  pure r

-- | Take a resource from the pool, following the same results as
-- 'withResource'.
--
-- /Note:/ this function returns both a resource and the 'LocalPool' it came
-- from so that it may either be destroyed (via 'destroyResource') or returned
-- to the pool (via 'putResource').
takeResource :: Pool a -> IO (a, LocalPool a)
takeResource pool = mask_ $ do
  lp <- getLocalPool (localPools pool)
  stripe <- takeMVar (stripeVar lp)
  if available stripe == 0
    then do
      q <- newEmptyMVar
      putMVar (stripeVar lp) $! stripe { queueR = Queue q (queueR stripe) }
      waitForResource (stripeVar lp) q >>= \case
        Just a  -> pure (a, lp)
        Nothing -> do
          a <- createResource pool `onException` restoreSize (stripeVar lp)
          pure (a, lp)
    else case cache stripe of
      [] -> do
        putMVar (stripeVar lp) $! stripe { available = available stripe - 1 }
        a <- createResource pool `onException` restoreSize (stripeVar lp)
        pure (a, lp)
      Entry a _ : as -> do
        putMVar (stripeVar lp) $! stripe
         { available = available stripe - 1
         , cache = as
         }
        pure (a, lp)

{-# DEPRECATED createPool "Use newPool instead" #-}
-- | Provided for compatibility with @resource-pool < 0.3@.
--
-- Use 'newPool' instead.
createPool :: IO a -> (a -> IO ()) -> Int -> NominalDiffTime -> Int -> IO (Pool a)
createPool create free numStripes idleTime maxResources = do
  newPool create free (realToFrac idleTime) (numStripes * maxResources)
