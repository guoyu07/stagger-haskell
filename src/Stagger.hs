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
  Dist.addSingleton,
) where

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent (forkIO)
import Control.Concurrent.STM (TVar, atomically)
import Control.Monad (forever, join)
import Control.Monad.Extra (whileM)
import Control.Monad.Trans (liftIO)
import Data.Monoid (Monoid(..))
import Data.Semigroup (Sum(..), Max(..), Min(..))
import Data.Serialize (encode)
import Data.String (fromString)
import Data.Text.Encoding (encodeUtf8)
import Data.Traversable (sequence)
import Prelude hiding (sequence)
import Stagger.Counter (Counter, incCounter, newCounter, readCounter)
import Stagger.Dist (DistValue(DistValue), Dist, newDist)
import Stagger.SocketUtil (recvMessage, withSocket)
import Stagger.Util (mapMaybeHashMap)

import qualified Control.Concurrent.STM as STM
import qualified Control.Monad.Trans.State as State
import qualified Data.ByteString as B
import qualified Data.HashMap.Strict as HM
import qualified Data.List.NonEmpty as NE
import qualified Data.Map as M
import qualified Data.MessagePack as Msg
import qualified Data.Text as T
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import qualified Stagger.Dist as Dist
import qualified Stagger.Protocol as Protocol

type MetricName = T.Text

data StaggerOpts =
  StaggerOpts {
    staggerHost :: NS.HostName,
    staggerPort :: NS.PortNumber,
    staggerTags :: M.Map B.ByteString B.ByteString
  }

defaultOpts :: StaggerOpts
defaultOpts = StaggerOpts "127.0.0.1" 5865 M.empty

data Count =
  Cumulative Integer |
  Current Double

getCumulative :: Count -> Maybe Integer
getCumulative (Cumulative x) = Just x
getCumulative _ = Nothing

getCurrent :: Count -> Maybe Double
getCurrent (Current x) = Just x
getCurrent _ = Nothing

data Stagger =
  Stagger
    !(TVar (IO (HM.HashMap MetricName Count)))
    !(TVar (HM.HashMap MetricName (IO (Maybe DistValue))))

makeCounts :: HM.HashMap MetricName Double -> Msg.Object
makeCounts =
  Msg.ObjectArray .
  map (uncurry makeCount) .
  HM.toList
 where
  makeCount :: MetricName -> Double -> Msg.Object
  makeCount name value =
    Msg.ObjectMap $ M.fromList [
      (Msg.ObjectString "Name", Msg.ObjectString $ encodeUtf8 name),
      (Msg.ObjectString "Count", Msg.ObjectDouble value)
    ]

makeDists :: HM.HashMap MetricName DistValue -> Msg.Object
makeDists =
  Msg.ObjectArray .
  map (uncurry makeDist) .
  HM.toList
 where
  makeDist :: MetricName -> DistValue -> Msg.Object
  makeDist
    name
    (DistValue (Sum weight) (Min min) (Max max) (Sum sum) (Sum sum_2)) =
    Msg.ObjectMap $ M.fromList [
      (Msg.ObjectString "Name", Msg.ObjectString $ encodeUtf8 name),
      (Msg.ObjectString "Dist", Msg.ObjectArray $
        map Msg.ObjectDouble [weight, min, max, sum, sum_2])
    ]

newStagger :: StaggerOpts -> IO Stagger
newStagger opts = do
  counts <- STM.newTVarIO (return HM.empty)
  dists <- STM.newTVarIO mempty
  let stagger = Stagger counts dists
  forkIO $ staggerThread stagger
  return stagger
 where
  sendRegistration :: NS.Socket -> IO ()
  sendRegistration sock = do
    NSB.send sock $
      encode $
      Protocol.RegisterProcessMessage $
      Protocol.RegisterProcess $
      staggerTags opts
    return ()

  staggerThread :: Stagger -> IO ()
  staggerThread (Stagger counts dists) =
    withSocket (staggerHost opts) (staggerPort opts) $ \sock -> do
      sendRegistration sock
      flip State.evalStateT HM.empty $ whileM $ do
        command <- recvMessage sock
        case command of
          Right (Protocol.ReportAllMessage (Protocol.ReportAll ts)) -> do
            prevCumulatives <- State.get
            newCountValues <- liftIO $ join $ atomically $ STM.readTVar counts
            let newCumulatives = mapMaybeHashMap getCumulative newCountValues
            State.put $ HM.union newCumulatives prevCumulatives

            let diff = HM.intersectionWith (-) newCumulatives prevCumulatives
            let newCurrents = mapMaybeHashMap getCurrent newCountValues
            let counts = HM.union newCurrents $ HM.map fromIntegral diff

            dists' <- liftIO $ atomically $ STM.readTVar dists
            dists'' <- liftIO $ sequence dists'
            let dists''' = mapMaybeHashMap id dists''

            let
              reply = Protocol.StatsCompleteMessage $
                Msg.ObjectMap $ M.fromList [
                  (Msg.ObjectString "Timestamp", Msg.ObjectUInt ts),
                  (Msg.ObjectString "Counts", makeCounts counts),
                  (Msg.ObjectString "Dists", makeDists dists''')
                ]
            liftIO $ NSB.send sock (encode reply)
            return True
          Left e -> return False

newDistMetric :: Stagger -> MetricName -> IO Dist
newDistMetric (Stagger _ dists) name = do
  dist <- newDist
  atomically $ STM.modifyTVar' dists $ HM.insert name $ Dist.getAndReset dist
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
  registerCount stagger name (Cumulative <$> readCounter counter)

registerCurrentCount :: Stagger -> MetricName -> Counter -> IO ()
registerCurrentCount stagger name counter =
  registerCount stagger name ((Current . fromIntegral) <$> readCounter counter)

registerCount :: Stagger -> MetricName -> IO Count -> IO ()
registerCount stagger name op =
  registerCounts stagger $ HM.singleton name <$> op

registerCounts :: Stagger -> IO (HM.HashMap MetricName Count) -> IO ()
registerCounts (Stagger counts _) op =
  atomically $ STM.modifyTVar' counts (\op' -> HM.union <$> op <*> op')
