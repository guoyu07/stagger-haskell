{-# LANGUAGE DeriveGeneric #-}

module Stagger.Dist (
  Dist,
  DistValue(..),
  newDist,
  getAndReset,
  addSingleton
) where

import Control.Applicative ((<$>), (<*>))
import Control.Arrow ((&&&))
import Control.DeepSeq (NFData)
import Data.IORef (IORef, newIORef)
import Data.Semigroup (Semigroup, Max(..), Min(..), Sum(..), (<>), Option(..))
import GHC.Generics (Generic)
import Stagger.Util (atomicModifyIORefNF)

import qualified Data.Semigroup as SG

data DistValue =
  DistValue
    !(Sum Double)
    !(Min Double)
    !(Max Double)
    !(Sum Double)
    !(Sum Double)
  deriving Generic

singleton :: Double -> DistValue
singleton x =
  DistValue
    (Sum 1)
    (Min x)
    (Max x)
    (Sum x)
    (Sum $ x^2)

instance Semigroup DistValue where
  (DistValue a b c d e) <> (DistValue v w x y z) =
    DistValue
      (a <> v)
      (b <> w)
      (c <> x)
      (d <> y)
      (e <> z)

instance NFData DistValue

newtype Dist =
  Dist (IORef (Option DistValue))

newDist :: IO Dist
newDist =
  Dist <$> newIORef (Option Nothing)

addSingleton :: Dist -> Double -> IO ()
addSingleton (Dist ref) value =
  atomicModifyIORefNF ref $ (<> (Option $ Just $ singleton value)) &&& const ()

getAndReset :: Dist -> IO (Maybe DistValue)
getAndReset (Dist ref) =
  atomicModifyIORefNF ref $ const (Option Nothing) &&& SG.getOption
