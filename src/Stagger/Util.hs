module Stagger.Util where

import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import Data.IORef (IORef, atomicModifyIORef)
import Data.Maybe (fromJust, isJust)
import Control.DeepSeq (NFData, deepseq)
import qualified Data.HashMap.Strict as HM
import Control.Concurrent (threadDelay)
import Control.Exception (IOException, handle)
import Control.Monad (forever)

catMaybesHashMap :: (Hashable k, Eq k) => HashMap k (Maybe x) -> HashMap k x
catMaybesHashMap = HM.map fromJust . HM.filter isJust

mapMaybeHashMap :: (Hashable k, Eq k) => (a -> Maybe b) -> HashMap k a -> HashMap k b
mapMaybeHashMap f = catMaybesHashMap . HM.map f

-- atomicModifyIORef wrapper that stores the data being put into the IORef in normal form
atomicModifyIORefNF :: (NFData a, NFData b) => IORef a -> (a -> (a, b)) -> IO b
atomicModifyIORefNF ref f = do
  b <- atomicModifyIORef ref $ \a ->
    case f a of
      v@(a',_) -> a' `deepseq` v
  b `deepseq` return b

eitherToMonad :: Monad m => Either String a -> m a
eitherToMonad = either (fail . show) return

foreverWithResource :: IO a -> (a -> IO ()) -> (a -> IO Bool) -> IO ()
foreverWithResource setup cleanup action =
  forever $ do
    handle handler $ do
      resource <- setup
      handle handler $
        while $ action resource
      cleanup resource
    threadDelay 1000000 -- 1 second
 where
  handler :: IOException -> IO ()
  handler e = print $ "stagger: foreverWithResource: " ++ show e

while :: IO Bool -> IO ()
while action = do
  result <- action
  case result of
    True -> while action
    False -> return ()

