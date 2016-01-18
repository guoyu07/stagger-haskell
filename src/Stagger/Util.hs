module Stagger.Util where

import Control.Concurrent (threadDelay)
import Control.DeepSeq (NFData, deepseq)
import Control.Exception (IOException, handle)
import Control.Monad (forever, when)
import Data.HashMap.Strict (HashMap)
import Data.Hashable (Hashable)
import Data.IORef (IORef, atomicModifyIORef)
import Data.Maybe (fromJust, isJust)

import qualified Data.HashMap.Strict as HM

catMaybesHashMap :: (Hashable k, Eq k) => HashMap k (Maybe x) -> HashMap k x
catMaybesHashMap = HM.map fromJust . HM.filter isJust

mapMaybeHashMap
  :: (Hashable k, Eq k)
  => (a -> Maybe b)
  -> HashMap k a
  -> HashMap k b
mapMaybeHashMap f = catMaybesHashMap . HM.map f

-- atomicModifyIORef wrapper that stores the data being put into the IORef in
-- normal form
atomicModifyIORefNF :: (NFData a, NFData b) => IORef a -> (a -> (a, b)) -> IO b
atomicModifyIORefNF ref f = do
  b <- atomicModifyIORef ref $ \a ->
    case f a of
      v@(a',_) -> a' `deepseq` v
  b `deepseq` return b

eitherToMonad :: Monad m => Either String a -> m a
eitherToMonad = either (fail . show) return

withRetryingResource :: IO a -> (a -> IO ()) -> (a -> IO ()) -> IO ()
withRetryingResource setup cleanup action =
  forever $ do
    handle handler $ do
      resource <- setup
      handle handler $
        action resource
      cleanup resource
    threadDelay 1000000 -- 1 second
 where
  handler :: IOException -> IO ()
  handler e = return ()

while :: Monad m => m Bool -> m ()
while action = do
  result <- action
  when result $ while action
