module Stagger.Counter (
  Counter,
  newCounter,
  incCounter,
  readCounter,
  setCounter
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

-- | Sets the counter value. This is only ever useful with a 'Current'
-- counter. Unfortunately, there is no easy way to enforce that at the type
-- level at the time being.
setCounter :: Counter -> Integer -> IO ()
setCounter (Counter c) n =
  atomicModifyIORefNF c (const (n, ()))

readCounter :: Counter -> IO Integer
readCounter (Counter c) =
  readIORef c
