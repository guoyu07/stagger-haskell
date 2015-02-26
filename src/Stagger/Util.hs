module Stagger.Util where

import Data.Hashable (Hashable)
import Data.HashMap.Strict (HashMap)
import Data.Maybe (fromJust, isJust)
import qualified Data.HashMap.Strict as HM

catMaybesHashMap :: (Hashable k, Eq k) => HashMap k (Maybe x) -> HashMap k x
catMaybesHashMap = HM.map fromJust . HM.filter isJust

mapMaybeHashMap :: (Hashable k, Eq k) => (a -> Maybe b) -> HashMap k a -> HashMap k b
mapMaybeHashMap f = catMaybesHashMap . HM.map f
