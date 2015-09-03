module Main where

import Control.Concurrent
import Control.Monad (forever)

import System.Random

import Stagger

main :: IO ()
main = do
  stagger <- newStagger defaultOpts
  counterA <- newCurrentCounter stagger "test_count_a"
  counterB <- newRateCounter stagger "test_count_b"
  distA <- newDistMetric stagger "test_dist_a"
  distB <- newDistMetric stagger "test_dist_b"

  forever $ do
    r <- randomRIO (0, 100000)
    threadDelay r
    addSingleton distA $ fromIntegral r
    addSingleton distB $ fromIntegral (100000 - r)
    incCounter counterA
    incCounter counterB
