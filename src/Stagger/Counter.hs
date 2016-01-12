module Stagger.Counter (
  Counter,
  newCounter,
  incCounter,
  readCounter,
) where

import Control.Applicative ((<$>), (<*>))
import Control.Arrow ((&&&))
import Data.IORef (IORef, newIORef, readIORef)
import Stagger.Util (atomicModifyIORefNF)

newtype Counter =
  Counter (IORef Integer)

newCounter :: IO Counter
newCounter =
  Counter <$> newIORef 0

decCounter :: Counter -> IO ()
decCounter (Counter c) =
  atomicModifyIORefNF c ((+1) &&& const ())

incCounter :: Counter -> IO ()
incCounter (Counter c) =
  atomicModifyIORefNF c ((+1) &&& const ())

readCounter :: Counter -> IO Integer
readCounter (Counter c) =
  readIORef c
