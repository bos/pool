module Data.Pool.WaiterQueue
  ( WaiterQueue,
    newQueueIO,
    push,
    pop,
  )
where

import Control.Concurrent.STM

-- | A FIFO queue that supports removing any element from the queue.
--
-- We have a pointer to the head of the list, and a pointer to the
-- final forward pointer in the list.
data WaiterQueue a
  = WaiterQueue
      (TVar (TDList a))
      (TVar (TVar (TDList a)))

-- | Each element has a pointer to the previous element's forward
-- pointer where "previous element" can be a 'TDList' cons cell or the
-- 'WaiterQueue' head pointer.
data TDList a
  = TCons
      (TVar (TVar (TDList a)))
      a
      (TVar (TDList a))
  | TNil

newQueueIO :: IO (WaiterQueue a)
newQueueIO = do
  emptyVarL <- newTVarIO TNil
  emptyVarR <- newTVarIO emptyVarL
  pure (WaiterQueue emptyVarL emptyVarR)

removeSelf ::
  -- | 'WaiterQueue's final forward pointer pointer
  TVar (TVar (TDList a)) ->
  -- | Our back pointer
  TVar (TVar (TDList a)) ->
  -- | Our forward pointer
  TVar (TDList a) ->
  STM ()
removeSelf tv prevPP nextP = do
  prevP <- readTVar prevPP
  -- If our back pointer points to our forward pointer then we have
  -- already been removed from the queue
  case prevP == nextP of
    True -> pure ()
    False -> do
      next <- readTVar nextP
      writeTVar prevP next
      case next of
        TNil -> writeTVar tv prevP
        TCons bp _ _ -> writeTVar bp prevP
      writeTVar prevPP nextP
{-# INLINE removeSelf #-}

-- | Returns an STM action that removes the pushed element from the
-- queue
push :: WaiterQueue a -> a -> STM (STM ())
push (WaiterQueue _ tv) a = do
  fwdPointer <- readTVar tv
  backPointer <- newTVar fwdPointer
  emptyVar <- newTVar TNil
  let cell = TCons backPointer a emptyVar
  writeTVar fwdPointer cell
  writeTVar tv emptyVar
  pure (removeSelf tv backPointer emptyVar)
{-# INLINE push #-}

pop :: WaiterQueue a -> STM (Maybe a)
pop (WaiterQueue hv tv) = do
  firstElem <- readTVar hv
  case firstElem of
    TNil -> pure Nothing
    TCons bp a fp -> do
      f <- readTVar fp
      writeTVar hv f
      case f of
        TNil -> writeTVar tv hv
        TCons fbp _ _ -> writeTVar fbp hv
      -- point the back pointer to the forward pointer as a sign that
      -- the cell has been popped (referenced in removeSelf)
      writeTVar bp fp
      pure (Just a)
{-# INLINE pop #-}
