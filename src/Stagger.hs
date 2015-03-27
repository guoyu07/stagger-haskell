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

import Data.Int
import Data.String (fromString)
import qualified Data.Text as T
import qualified Data.List.NonEmpty as NE
import qualified Data.HashMap.Strict as HM
import qualified Data.Map as M
import Data.Serialize (Serialize, Get, encode, decode, get, put)
import Data.Monoid (Monoid(..))
import Data.Traversable (sequence)
import Data.Semigroup (Sum(..), Max(..), Min(..))
import qualified Data.MessagePack as Msg

import Control.Applicative ((<$>), (<*>))
import Control.Monad (forever, join)
import Control.Monad.Trans (liftIO)
import qualified Control.Monad.Trans.State as State
import Control.Concurrent
import Control.Concurrent.STM

import System.ZMQ3 as ZMQ

import Stagger.Util (mapMaybeHashMap)

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
    !Int64

instance Serialize ReportAll where
  get = (get :: Get Msg.Object) >>= (maybe (fail "failed to unpack") return . fromObjReportAll)
   where
    fromObjReportAll :: Msg.Object -> Maybe ReportAll
    fromObjReportAll m = ReportAll <$> (fromObjInteger =<< lookup "Timestamp" =<< getMap m)

    fromObjInteger :: Msg.Object -> Maybe Int64
    fromObjInteger (Msg.ObjectInt i) = Just i
    fromObjInteger _ = Nothing

    getMap :: Msg.Object -> Maybe [(T.Text, Msg.Object)]
    getMap (Msg.ObjectMap elems) =
      mapM (\(k, v) -> do
        k' <- fromObjText k
        return (k', v)) (M.toList elems)
    getMap _ = Nothing

    fromObjText :: Msg.Object -> Maybe T.Text
    fromObjText (Msg.ObjectString r) = Just r
    fromObjText _ = Nothing

  put (ReportAll r) = put $ Msg.ObjectMap $ M.fromList [
      (Msg.ObjectString "Timestamp", Msg.ObjectInt r)
    ]

makeCounts :: HM.HashMap MetricName Double -> Msg.Object
makeCounts =
  Msg.ObjectArray .
  map (uncurry makeCount) .
  HM.toList
 where
  makeCount :: MetricName -> Double -> Msg.Object
  makeCount name value =
    Msg.ObjectMap $ M.fromList [
      (Msg.ObjectString "Name", Msg.ObjectString name),
      (Msg.ObjectString "Count", Msg.ObjectDouble value)
    ]

makeDists :: HM.HashMap MetricName DistValue -> Msg.Object
makeDists =
  Msg.ObjectArray .
  map (uncurry makeDist) .
  HM.toList
 where
  makeDist :: MetricName -> DistValue -> Msg.Object
  makeDist name (DistValue (Sum weight) (Min min) (Max max) (Sum sum) (Sum sum_2)) =
    Msg.ObjectMap $ M.fromList [
      (Msg.ObjectString "Name", Msg.ObjectString name),
      (Msg.ObjectString "Dist", Msg.ObjectArray $ map Msg.ObjectDouble [weight, min, max, sum, sum_2])
    ]

newStagger :: StaggerOpts -> IO Stagger
newStagger opts = do
  counts <- newTVarIO (return HM.empty)
  dists <- newTVarIO mempty

  forkIO $ ZMQ.withContext $ \context ->
    ZMQ.withSocket context ZMQ.Pair $ \pair -> do
      ZMQ.bind pair "tcp://127.0.0.1:*"
      pairEndpoint <- takeWhile (/= '\NUL') <$> ZMQ.lastEndpoint pair

      registration <- ZMQ.socket context ZMQ.Push
      ZMQ.connect registration $ registrationAddr opts

      ZMQ.sendMulti registration $ NE.fromList [fromString pairEndpoint, ""]

      flip State.evalStateT HM.empty $ forever $ do
        [type_, data_] <- liftIO $ ZMQ.receiveMulti pair
        case type_ of
          "report_all" -> do
            let ReportAll ts = either error id (decode data_)

            prevCummulatives <- State.get
            newCountValues <- liftIO $ join $ atomically $ readTVar counts
            let newCummulatives = mapMaybeHashMap getCummulative newCountValues
            State.put $ HM.union newCummulatives prevCummulatives

            let diff = HM.intersectionWith (-) newCummulatives prevCummulatives
            let newCurrents = mapMaybeHashMap getCurrent newCountValues
            let counts = HM.union newCurrents $ HM.map fromIntegral diff

            dists' <- liftIO $ atomically $ readTVar dists
            dists'' <- liftIO $ sequence dists'
            let dists''' = mapMaybeHashMap id dists''

            let
              reply =
                Msg.ObjectMap $ M.fromList [
                  (Msg.ObjectString "Timestamp", Msg.ObjectInt ts),
                  (Msg.ObjectString "Counts", makeCounts counts),
                  (Msg.ObjectString "Dists", makeDists dists''')
                ]
            liftIO $ sendMulti pair $ NE.fromList ["stats_complete", encode reply]
          "pair:shutdown" -> do
            liftIO $ print "shutdown"
            liftIO $ ZMQ.sendMulti registration $ NE.fromList [fromString pairEndpoint, ""]
          _ -> liftIO $ print ("unknown", type_, data_)

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
