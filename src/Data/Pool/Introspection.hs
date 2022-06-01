-- | A variant of "Data.Pool" with introspection capabilities.
module Data.Pool.Introspection
  ( -- * Pool
    PoolConfig(..)
  , Pool
  , LocalPool
  , newPool

    -- * Resource management
  , Resource(..)
  , Acquisition(..)
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
  , availableResources :: !Int
  , acquisition        :: !Acquisition
  , acquisitionTime    :: !Double
  , creationTime       :: !(Maybe Double)
  } deriving (Eq, Show, Generic)

-- | Describes how a resource was acquired from the pool.
data Acquisition
  = Immediate
  -- ^ A resource was taken from the pool immediately.
  | Delayed
  -- ^ The thread had to wait until a resource was released.
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
          let res = Resource
                { resource           = a
                , stripeNumber       = stripeId lp
                , availableResources = 0
                , acquisition        = Delayed
                , acquisitionTime    = t2 - t1
                , creationTime       = Nothing
                }
          pure (res, lp)
        Nothing -> do
          t2 <- getMonotonicTime
          a  <- createResource (poolConfig pool) `onException` restoreSize (stripeVar lp)
          t3 <- getMonotonicTime
          let res = Resource
                { resource           = a
                , stripeNumber       = stripeId lp
                , availableResources = 0
                , acquisition        = Delayed
                , acquisitionTime    = t2 - t1
                , creationTime       = Just $! t3 - t2
                }
          pure (res, lp)
    else case cache stripe of
      [] -> do
        let newAvailable = available stripe - 1
        putMVar (stripeVar lp) $! stripe { available = newAvailable }
        t2 <- getMonotonicTime
        a  <- createResource (poolConfig pool) `onException` restoreSize (stripeVar lp)
        t3 <- getMonotonicTime
        let res = Resource
              { resource           = a
              , stripeNumber       = stripeId lp
              , availableResources = newAvailable
              , acquisition        = Immediate
              , acquisitionTime    = t2 - t1
              , creationTime       = Just $! t3 - t2
              }
        pure (res, lp)
      Entry a _ : as -> do
        let newAvailable = available stripe - 1
        putMVar (stripeVar lp) $! stripe { available = newAvailable, cache = as }
        t2 <- getMonotonicTime
        let res = Resource
              { resource           = a
              , stripeNumber       = stripeId lp
              , availableResources = newAvailable
              , acquisition        = Immediate
              , acquisitionTime    = t2 - t1
              , creationTime       = Nothing
              }
        pure (res, lp)
