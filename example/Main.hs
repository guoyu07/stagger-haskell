module Main where

import Control.Concurrent (threadDelay)
import Control.Monad (forever)
import System.Random (randomRIO)

import qualified Stagger as S

main :: IO ()
main = do
  stagger <- S.newStagger S.defaultOpts
  counterA <- S.newCurrentCounter stagger "test_count_a"
  counterB <- S.newRateCounter stagger "test_count_b"
  distA <- S.newDistMetric stagger "test_dist_a"
  distB <- S.newDistMetric stagger "test_dist_b"

  forever $ do
    r <- randomRIO (0, 100000)
    threadDelay r
    S.addSingleton distA $ fromIntegral r
    S.addSingleton distB $ fromIntegral (100000 - r)
    S.incCounter counterA
    S.incCounter counterB
