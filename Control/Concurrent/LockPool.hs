{-# LANGUAGE BangPatterns #-}

module Control.Concurrent.LockPool
  ( LockPool
  , LockingStats(..)
  , withLockPool
  , withLockPoolStats
  , incrementLockPoolSize
  , decrementLockPoolSize
  , readLockPoolSize
  , newLockPool
  ) where

import Control.Monad.STM
import Control.Concurrent.STM.TVar
import Control.Exception (bracket_,bracket)
import qualified System.Clock as CLK

-- | A LockPool lets us set an upper bound on the number of threads
--   that are doing something simultaneously.
data LockPool = LockPool 
  Int        -- ^ absolute maximum holders
  (TVar Int) -- ^ maximum number of lock holders
  (TVar Int) -- ^ current number of lock holders

data Clocked a = Clocked !Integer !a

-- | The result of a computation along with some information
--   about how long we had to wait to acquire a lock and how
--   long the computation took.
data LockingStats = LockingStats
  { lsWaitedNanoseconds :: !Integer
  , lsActionNanoseconds :: !Integer
  , lsTakenLocks :: !Int
    -- ^ Count of taken locks before releasing the lock
  , lsTotalLocks :: !Int 
    -- ^ Count of total locks before adjusting the total
  }

stopwatch :: IO a -> IO (Clocked a)
stopwatch x = do
  t1 <- CLK.getTime CLK.Monotonic
  a <- x
  t2 <- CLK.getTime CLK.Monotonic
  return (Clocked (CLK.toNanoSecs (CLK.diffTimeSpec t2 t1)) a)

stopwatch_ :: IO a -> IO Integer
stopwatch_ x = do
  t1 <- CLK.getTime CLK.Monotonic
  _ <- x
  t2 <- CLK.getTime CLK.Monotonic
  return (CLK.toNanoSecs (CLK.diffTimeSpec t2 t1))

withLockPool :: LockPool -> IO a -> IO a
withLockPool (LockPool _ maxHoldersVar currentHoldersVar) action = 
  bracket_ acquire release action
  where
  acquire = atomically $ do
    maxHolders <- readTVar maxHoldersVar
    currentHolders <- readTVar currentHoldersVar
    check (currentHolders < maxHolders)
    writeTVar currentHoldersVar $! (currentHolders + 1)
  release = atomically $ modifyTVar' currentHoldersVar (subtract 1)

withLockPoolStats :: LockPool -> IO a -> IO (LockingStats,a)
withLockPoolStats (LockPool _ maxHoldersVar currentHoldersVar) x = 
  bracket acquire release action
  where
  acquire = stopwatch_ $ atomically $ do
    maxHolders <- readTVar maxHoldersVar
    currentHolders <- readTVar currentHoldersVar
    check (currentHolders < maxHolders)
    writeTVar currentHoldersVar $! (currentHolders + 1)
  action waitedNs = do
    Clocked actionNs a <- stopwatch x
    taken <- readTVarIO currentHoldersVar
    total <- readTVarIO maxHoldersVar
    return (LockingStats waitedNs actionNs taken total, a)
  release _ = atomically $ modifyTVar' currentHoldersVar (subtract 1)

-- | We do not allow the size to drop to zero since that
--   would halt all progress.
decrementLockPoolSize :: LockPool -> IO Int
decrementLockPoolSize (LockPool _ maxHoldersVar _) = atomically $ do
  i <- readTVar maxHoldersVar
  let i' = max 1 (i - 1)
  writeTVar maxHoldersVar $! i'
  return i'

incrementLockPoolSize :: LockPool -> IO Int
incrementLockPoolSize (LockPool absMax maxHoldersVar _) = atomically $ do
  i <- readTVar maxHoldersVar
  let i' = min absMax (i + 1)
  writeTVar maxHoldersVar $! i'
  return i'

readLockPoolSize :: LockPool -> IO Int
readLockPoolSize (LockPool _ maxHoldersVar _) = 
  readTVarIO maxHoldersVar

_readLockPoolHolds :: LockPool -> IO Int
_readLockPoolHolds (LockPool _ _ currentHoldersVar) = 
  readTVarIO currentHoldersVar

newLockPool :: 
     Int -- ^ absolute maximum number of holders
  -> IO LockPool
newLockPool n = LockPool n
  <$> newTVarIO 1
  <*> newTVarIO 0

-- setLockPoolSize :: LockPool -> Int -> IO ()
-- setLockPoolSize (LockPool maxHoldersVar _) !newMax =
--   atomically (writeTVar maxHoldersVar newMax)
