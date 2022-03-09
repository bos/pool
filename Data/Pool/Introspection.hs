-- | A variant of "Data.Pool" with introspection capabilities.
module Data.Pool.Introspection
  ( -- * Pool
    Pool
  , LocalPool
  , newPool

    -- * Resource management
  , Resource(..)
  , AcquisitionMethod(..)
  , withResource
  , takeResource
  , putResource
  , destroyResource
  , destroyAllResources
  ) where

import Control.Concurrent
import Control.Exception
import GHC.Clock

import Data.Pool.Internal

-- | 'Data.Pool.withResource' with introspection capabilities.
withResource :: Pool a -> (Resource a -> IO r) -> IO r
withResource pool act = mask $ \unmask -> do
  (res, localPool) <- takeResource pool
  r <- unmask (act res) `onException` destroyResource pool localPool (resource res)
  putResource localPool (resource res)
  pure r

-- | 'Data.Pool.takeResource' with introspection capabilities.
takeResource :: Pool a -> IO (Resource a, LocalPool a)
takeResource pool = mask_ $ do
  t1 <- getMonotonicTime
  localPool@(LocalPool mstripe) <- getLocalPool (localPools pool)
  stripe <- takeMVar mstripe
  if available stripe == 0
    then do
      q <- newEmptyMVar
      putMVar mstripe $! stripe { queueR = Queue q (queueR stripe) }
      waitForResource mstripe q >>= \case
        Just a -> do
          t2 <- getMonotonicTime
          pure (Resource a (t2 - t1) (WaitedThen Taken) 0, localPool)
        Nothing -> do
          a  <- createResource pool `onException` restoreSize mstripe
          t2 <- getMonotonicTime
          pure (Resource a (t2 - t1) (WaitedThen Created) 0, localPool)
    else case cache stripe of
      [] -> do
        let newAvailable = available stripe - 1
        putMVar mstripe $! stripe { available = newAvailable }
        a  <- createResource pool `onException` restoreSize mstripe
        t2 <- getMonotonicTime
        pure (Resource a (t2 - t1) Created newAvailable, localPool)
      Entry a _ : as -> do
        let newAvailable = available stripe - 1
        putMVar mstripe $! stripe { available = newAvailable, cache = as }
        t2 <- getMonotonicTime
        pure (Resource a (t2 - t1) Taken newAvailable, localPool)
