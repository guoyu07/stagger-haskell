module Main where

import Control.Concurrent
import Control.Monad (forever)

import System.Random

import Stagger

main :: IO ()
main = do
  stagger <- newStagger defaultOpts
  counter <- newRateCounter stagger "test"
  dist <- newDistMetric stagger "test_dist"

  forever $ do
    r <- randomRIO (0, 100000)
    threadDelay r
    addSingleton dist $ fromIntegral r
    incCounter counter
