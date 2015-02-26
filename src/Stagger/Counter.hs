module Stagger.Counter (
  Counter,
  newCounter,
  incCounter,
  readCounter,
) where

import Data.IORef

import Control.Arrow ((&&&))
import Control.Applicative ((<$>), (<*>))

newtype Counter =
  Counter (IORef Integer)

newCounter :: IO Counter
newCounter =
  Counter <$> newIORef 0

decCounter :: Counter -> IO ()
decCounter (Counter c) =
  atomicModifyIORef c ((+1) &&& (const ()))

incCounter :: Counter -> IO ()
incCounter (Counter c) =
  atomicModifyIORef c ((+1) &&& (const ()))

readCounter :: Counter -> IO Integer
readCounter (Counter c) =
  readIORef c
