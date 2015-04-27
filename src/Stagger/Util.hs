module Stagger.Util where

import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import Data.IORef (IORef, atomicModifyIORef)
import Data.Maybe (fromJust, isJust)
import Control.DeepSeq (NFData, deepseq)
import qualified Data.HashMap.Strict as HM

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
