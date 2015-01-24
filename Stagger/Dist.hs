module Stagger.Dist (
  Dist,
  DistValue,
  newDist,
  getAndReset,
  addSingleton
) where

import Data.Semigroup
import Data.IORef

import Control.Arrow ((&&&))
import Control.Applicative ((<$>), (<*>))

type DistValue = (Sum Double, Min Double, Max Double, Sum Double, Sum Double)

singleton :: Double -> DistValue
singleton x = (Sum 1, Min x, Max x, Sum x, Sum $ x^2)

newtype Dist =
  Dist (IORef (Option DistValue))

newDist :: IO Dist
newDist =
  Dist <$> newIORef (Option Nothing)

addSingleton :: Dist -> Double -> IO ()
addSingleton (Dist ref) value =
  atomicModifyIORef ref $ (<> (Option $ Just $ singleton value)) &&& (const ())

getAndReset :: Dist -> IO (Maybe DistValue)
getAndReset (Dist ref) =
  atomicModifyIORef ref $ const (Option Nothing) &&& getOption
