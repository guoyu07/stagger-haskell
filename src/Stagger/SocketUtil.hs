module Stagger.SocketUtil where

import Control.Exception (IOException, bracket, handle)
import Control.Monad ((>=>))
import Control.Monad.Trans (MonadIO, liftIO)
import Data.Serialize (Result(..), get, runGetPartial)
import Stagger.Util (withRetryingResource)

import qualified Data.ByteString as B
import qualified Network.Socket as NS
import qualified Network.Socket.ByteString as NSB
import qualified Stagger.Protocol as Protocol

withSocket :: NS.HostName -> NS.PortNumber -> (NS.Socket -> IO ()) -> IO ()
withSocket host port action =
  withRetryingResource acquireSocket closeSocket (connectSocket >=> action)
 where
  acquireSocket :: IO (NS.AddrInfo, NS.Socket)
  acquireSocket = do
    addrs <- NS.getAddrInfo Nothing (Just host) (Just $ show port)
    let addr = head addrs
    sock <- NS.socket (NS.addrFamily addr) NS.Stream NS.defaultProtocol
    return (addr, sock)

  -- Connect must be done in the "action" instead of setup, so that exception
  -- cleanup is applied
  connectSocket :: (NS.AddrInfo, NS.Socket) -> IO NS.Socket
  connectSocket (addr, sock) = do
    NS.connect sock (NS.addrAddress addr)
    return sock

  closeSocket :: (NS.AddrInfo, NS.Socket) -> IO ()
  closeSocket (_, sock) = do
    -- Try and shutdown the socket if it is still open
    handle handler $ NS.shutdown sock NS.ShutdownBoth
    NS.close sock
   where
    handler :: IOException -> IO ()
    handler _ = return ()

recvMessage :: MonadIO m => NS.Socket -> m (Either String Protocol.Message)
recvMessage sock = recvMessageRec $ runGetPartial get
 where
  recvMessageRec
    :: MonadIO m
    => (B.ByteString
    -> Result Protocol.Message)
    -> m (Either String Protocol.Message)
  recvMessageRec parse = do
    dat <- liftIO $ NSB.recv sock 4096
    if B.null dat then
      return $ Left "Socket closed"
    else
      case parse dat of
        Done res _ -> do
          return $ Right res
        Partial partialParse -> recvMessageRec partialParse
        Fail e _ -> return $ Left $ "Failed to decode: " ++ e
