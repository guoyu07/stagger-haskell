{-# LANGUAGE OverloadedStrings #-}
module Stagger (
  StaggerOpts(..),
  defaultOpts,
  Stagger,
  newStagger,
  MetricName,
  Count(..),
  registerCounts,
  Counter,
  newRateCounter,
  newCurrentCounter,
  incCounter,
  Dist,
  newDistMetric,
  addSingleton,
) where

import Prelude hiding (sequence)

import Data.String (fromString)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.List.NonEmpty as NE
import qualified Data.HashMap.Strict as HM
import Data.Monoid (Monoid(..))
import Data.Traversable (sequence)
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
import Megabus.DataModel.Util (mapMaybeHashMap)

import Stagger.Counter
import Stagger.Dist

type MetricName = T.Text

data StaggerOpts = StaggerOpts { registrationAddr :: String }

defaultOpts :: StaggerOpts
defaultOpts = StaggerOpts { registrationAddr = "tcp://127.0.0.1:5867" }

data Count =
  Cummulative Integer |
  Current Double

getCummulative :: Count -> Maybe Integer
getCummulative (Cummulative x) = Just x
getCummulative _ = Nothing

getCurrent :: Count -> Maybe Double
getCurrent (Current x) = Just x
getCurrent _ = Nothing

data Stagger =
  Stagger
    !(TVar (IO (HM.HashMap MetricName Count)))
    !(TVar (HM.HashMap MetricName (IO (Maybe DistValue))))

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

makeCounts :: HM.HashMap MetricName Double -> Msg.Object
makeCounts =
  toObj .
  map (uncurry makeCount) .
  HM.toList
 where
  makeCount :: MetricName -> Double -> Msg.Object
  makeCount name value =
    Msg.ObjectMap [
      (objectText "Name", toObj name),
      (objectText "Count", toObj value)
    ]

makeDists :: HM.HashMap MetricName DistValue -> Msg.Object
makeDists =
  toObj .
  map (uncurry makeDist) .
  HM.toList
 where
  makeDist :: MetricName -> DistValue -> Msg.Object
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
                let ReportAll ts = Msg.unpack data_

                prevCummulatives <- get
                newCountValues <- liftIO $ join $ atomically $ readTVar counts
                let newCummulatives = mapMaybeHashMap getCummulative newCountValues
                put $ HM.union newCummulatives prevCummulatives

                let diff = HM.intersectionWith (-) newCummulatives prevCummulatives
                let newCurrents = mapMaybeHashMap getCurrent newCountValues
                let counts = HM.union newCurrents $ HM.map fromIntegral diff

                dists' <- liftIO $ atomically $ readTVar dists
                dists'' <- liftIO $ sequence dists'
                let dists''' = mapMaybeHashMap id dists''

                let
                  reply =
                    Msg.ObjectMap [
                      (objectText "Timestamp", toObj ts),
                      (objectText "Counts", makeCounts counts),
                      (objectText "Dists", makeDists dists''')
                    ]
                liftIO $ sendMulti pair $ NE.fromList ["stats_complete", BL.toStrict $ Msg.pack reply]
              "pair:shutdown" -> do
                liftIO $ print ("shutdown")
                liftIO $ ZMQ.sendMulti registration $ NE.fromList [fromString pairEndpoint, ""]
              _ -> do
                let data_' = Msg.unpack data_ :: Msg.Object
                liftIO $ print ("unknown", type_, data_')

  return $ Stagger counts dists

newDistMetric :: Stagger -> MetricName -> IO Dist
newDistMetric (Stagger _ dists) name = do
  dist <- newDist
  atomically $ modifyTVar' dists $ HM.insert name $ getAndReset dist
  return dist

newRateCounter :: Stagger -> MetricName -> IO Counter
newRateCounter stagger name = do
  counter <- newCounter
  registerRateCount stagger name counter
  return counter

newCurrentCounter :: Stagger -> MetricName -> IO Counter
newCurrentCounter stagger name = do
  counter <- newCounter
  registerCurrentCount stagger name counter
  return counter

registerRateCount :: Stagger -> MetricName -> Counter -> IO ()
registerRateCount stagger name counter =
  registerCount stagger name (Cummulative <$> readCounter counter)

registerCurrentCount :: Stagger -> MetricName -> Counter -> IO ()
registerCurrentCount stagger name counter =
  registerCount stagger name ((Current . fromIntegral) <$> readCounter counter)

registerCount :: Stagger -> MetricName -> IO Count -> IO ()
registerCount stagger name op =
  registerCounts stagger $ HM.singleton name <$> op

registerCounts :: Stagger -> IO (HM.HashMap MetricName Count) -> IO ()
registerCounts (Stagger counts _) op = do
  atomically $ modifyTVar' counts (\op' -> HM.union <$> op <*> op')
