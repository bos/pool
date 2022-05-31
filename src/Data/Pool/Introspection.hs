-- | A variant of "Data.Pool" with introspection capabilities.
module Data.Pool.Introspection
  ( -- * Pool
    PoolConfig(..)
  , Pool
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
import GHC.Generics (Generic)

import Data.Pool.Internal

-- | A resource taken from the pool along with additional information.
data Resource a = Resource
  { resource           :: a
  , stripeNumber       :: !Int
  , acquisitionTime    :: !Double
  , acquisitionMethod  :: !AcquisitionMethod
  , availableResources :: !Int
  } deriving (Eq, Show, Generic)

-- | Method of acquiring a resource from the pool.
data AcquisitionMethod
  = Created
  -- ^ A new resource was created.
  | Taken
  -- ^ An existing resource was directly taken from the pool.
  | WaitedThen !AcquisitionMethod
  -- ^ The thread had to wait until a resource was released. The inner method
  -- signifies whether the resource was returned to the pool via 'putResource'
  -- ('Taken') or 'destroyResource' ('Created').
  deriving (Eq, Show, Generic)

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
  lp <- getLocalPool (localPools pool)
  stripe <- takeMVar (stripeVar lp)
  if available stripe == 0
    then do
      q <- newEmptyMVar
      putMVar (stripeVar lp) $! stripe { queueR = Queue q (queueR stripe) }
      waitForResource (stripeVar lp) q >>= \case
        Just a -> do
          t2 <- getMonotonicTime
          pure (Resource a (stripeId lp) (t2 - t1) (WaitedThen Taken) 0, lp)
        Nothing -> do
          a  <- createResource (poolConfig pool) `onException` restoreSize (stripeVar lp)
          t2 <- getMonotonicTime
          pure (Resource a (stripeId lp) (t2 - t1) (WaitedThen Created) 0, lp)
    else case cache stripe of
      [] -> do
        let newAvailable = available stripe - 1
        putMVar (stripeVar lp) $! stripe { available = newAvailable }
        a  <- createResource (poolConfig pool) `onException` restoreSize (stripeVar lp)
        t2 <- getMonotonicTime
        pure (Resource a (stripeId lp) (t2 - t1) Created newAvailable, lp)
      Entry a _ : as -> do
        let newAvailable = available stripe - 1
        putMVar (stripeVar lp) $! stripe { available = newAvailable, cache = as }
        t2 <- getMonotonicTime
        pure (Resource a (stripeId lp) (t2 - t1) Taken newAvailable, lp)
