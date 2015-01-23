{-# LANGUAGE OverloadedStrings #-}
module Stagger (
  StaggerOpts(..),
  defaultOpts,
  Stagger,
  newStagger,
  singleton,
  registerCount,
  registerCounts,
  addDist,
) where

import Data.String (fromString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.List.NonEmpty as NE
import qualified Data.HashMap.Strict as HM
import Data.Monoid (Monoid(..))
import Data.Semigroup (Semigroup(..), Sum(..), Max(..), Min(..))

import Control.Applicative ((<$>), (<*>), (<*))
import Control.Monad (forever, join)
import Control.Monad.Trans (lift, liftIO)
import Control.Monad.Trans.State (evalStateT, get, put)
import Control.Concurrent
import Control.Concurrent.STM

import System.ZMQ3 as ZMQ

import qualified Data.MessagePack as Msg

import Megabus.ObjectConvertible

type Name = T.Text

data StaggerOpts = StaggerOpts { registrationAddr :: String }

defaultOpts :: StaggerOpts
defaultOpts = StaggerOpts { registrationAddr = "tcp://127.0.0.1:5867" }

type DistValue = (Sum Double, Min Double, Max Double, Sum Double, Sum Double)

singleton :: Double -> DistValue
singleton x = (Sum 1, Min x, Max x, Sum x, Sum $ x^2)

newtype DistValues =
  DistValues {
    unDistValues :: (HM.HashMap Name DistValue)
  }

data Stagger =
  Stagger
    !(TVar (IO (HM.HashMap Name Integer)))
    !(TVar DistValues)

instance Monoid DistValues where
  mempty = DistValues HM.empty

  mappend (DistValues a) (DistValues b) =
    DistValues
      (HM.unionWith (<>) a b)

data ReportAll =
  ReportAll
    !Integer

instance ObjectConvertible ReportAll where
  fromObj obj = do
    m <- getMap obj
    ReportAll <$> key "Timestamp" m

  toObj (ReportAll ts) =
    Msg.ObjectMap [
      (objectText "Timestamp", toObj ts)
    ]

instance Msg.Packable ReportAll where
  from = objFrom

instance Msg.Unpackable ReportAll where
  get = objGet

makeCounts :: HM.HashMap Name Integer -> Msg.Object
makeCounts =
  toObj .
  map (uncurry makeCount) .
  HM.toList
 where
  makeCount :: Name -> Integer -> Msg.Object
  makeCount name value =
    Msg.ObjectMap [
      (objectText "Name", toObj name),
      (objectText "Count", Msg.ObjectDouble $ fromIntegral value)
    ]

makeDists :: DistValues -> Msg.Object
makeDists =
  toObj .
  map (uncurry makeDist) .
  HM.toList .
  unDistValues
 where
  makeDist :: Name -> DistValue -> Msg.Object
  makeDist name (Sum weight, Min min, Max max, Sum sum, Sum sum_2) =
    Msg.ObjectMap [
      (objectText "Name", toObj name),
      (objectText "Dist", toObj [weight, min, max, sum, sum_2])
    ]

newStagger :: StaggerOpts -> IO Stagger
newStagger opts = do
  counts <- newTVarIO (return HM.empty)
  dists <- newTVarIO mempty

  forkIO $ do
    ZMQ.withContext $ \context -> do
      ZMQ.withSocket context ZMQ.Pair $ \pair -> do
        ZMQ.bind pair "tcp://127.0.0.1:*"
        pairEndpoint <- takeWhile (/= '\NUL') <$> ZMQ.lastEndpoint pair

        registration <- ZMQ.socket context ZMQ.Push
        ZMQ.connect registration $ registrationAddr opts

        ZMQ.sendMulti registration $ NE.fromList [fromString pairEndpoint, ""]

        flip evalStateT HM.empty $ do
          forever $ do
            [type_, data_] <- liftIO $ ZMQ.receiveMulti pair
            case type_ of
              "report_all" -> do
                let data_' = Msg.unpack data_ :: Msg.Object
                liftIO $ print data_'
                let ReportAll ts = Msg.unpack data_

                prevCountValues <- get
                newCountValues <- liftIO $ join $ atomically $ readTVar counts
                put $ HM.union newCountValues prevCountValues
                let counts = HM.intersectionWith (-) newCountValues prevCountValues

                dists' <- liftIO $ atomically $ readTVar dists <* writeTVar dists mempty

                let
                  reply =
                    Msg.ObjectMap [
                      (objectText "Timestamp", toObj ts),
                      (objectText "Counts", makeCounts counts),
                      (objectText "Dists", makeDists dists')
                    ]
                liftIO $ sendMulti pair $ NE.fromList ["stats_complete", BL.toStrict $ Msg.pack reply]
                liftIO $ print ("report_all", reply)
              "pair:shutdown" -> do
                liftIO $ print ("shutdown")
                liftIO $ ZMQ.sendMulti registration $ NE.fromList [fromString pairEndpoint, ""]
              _ -> do
                let data_' = Msg.unpack data_ :: Msg.Object
                liftIO $ print ("unknown", type_, data_')

  return $ Stagger counts dists

registerCount :: Stagger -> Name -> IO Integer -> IO ()
registerCount stagger name op =
  registerCounts stagger $ HM.singleton name <$> op

registerCounts :: Stagger -> IO (HM.HashMap Name Integer) -> IO ()
registerCounts (Stagger counts _) op = do
  atomically $ modifyTVar' counts (\op' -> HM.union <$> op <*> op')

addDist :: Stagger -> Name -> DistValue -> IO ()
addDist (Stagger _ dists) name value = do
  atomically $ modifyTVar' dists $ mappend $ DistValues $ HM.singleton name value
