{-# LANGUAGE BangPatterns, CPP, ScopedTypeVariables, PackageImports #-}

-- | Named Pipes implementation of the Transport API.
module Network.Transport.Pipes (createTransport) where

import Control.Monad (when, unless, zipWithM_, liftM)
import Control.Exception (IOException, handle, throwIO)
import Control.Concurrent.MVar
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent (threadDelay, forkOS)
import Data.IORef
import Data.Word
import Data.Map (Map)
import qualified Data.Map as Map
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import System.Random (randomIO)
import System.Posix.Files (createNamedPipe, unionFileModes,
                           ownerReadMode, ownerWriteMode)
import System.Posix.Types (Fd)
import System.Directory (removeFile)
import qualified "unix-bytestring" System.Posix.IO.ByteString as PIO
import System.Posix.IO as PIO (openFd, closeFd, OpenFileFlags(..),
                               OpenMode(ReadOnly, WriteOnly))
import Network.Transport
import Network.Transport.Internal (tryIO, encodeInt32, decodeInt32, void,
                                   asyncWhenCancelled, prependLength, tryToEnum)

-- EndPoint Map
type EndPointMap = (Map EndPointAddress (Chan Event))

-- Endpoint Pair (local endpoint, remote endpoint)
type EndPointPair = (EndPointAddress, EndPointAddress)

-- Connection Map
type ConnectionMap = (Map EndPointPair (ConnectionId))

-- Global transport state
data TransportState = State { endpoints   :: !EndPointMap
                            , connections :: !ConnectionMap
                            }

fileFlags :: OpenFileFlags
fileFlags = 
 PIO.OpenFileFlags {
    PIO.append    = False,
    PIO.exclusive = False,
    PIO.noctty    = False,
    PIO.nonBlock  = True,
    -- In nonblocking mode opening for read will always succeed and
    -- opening for write must happen second.
    PIO.trunc     = False
  }

-- | Control headers
data ControlHeader =
    -- | Tell the remote endpoint that we created a new connection
    NewConnection
    -- | Tell the remote endpoint we will no longer be using a connection
  | CloseConnection
    -- | Tell the remote endpoint that this is a data payload
  | Data
  deriving (Enum, Bounded, Show)

------------------------------------------------------------------------------

mkBackoff :: IO (IO ())
mkBackoff = 
  do tref <- newIORef 1
     return$ do t <- readIORef tref
                writeIORef tref (min maxwait (2 * t))
		threadDelay t
 where 
   maxwait = 50 * 1000

tryUntilNoIOErr :: IO a -> IO a
tryUntilNoIOErr action = mkBackoff >>= loop 
 where 
  loop bkoff = 
    handle (\ (_ :: IOException) -> 
	     do _ <- bkoff
	        loop bkoff) $ 
	   action

-- TODO: Handle errors
writeFd :: Fd -> [ByteString] -> IO ()
writeFd fd payload = do
  let msg = BSC.concat payload
  cnt <- PIO.fdWrite fd msg
  unless ((fromIntegral cnt) == (BSC.length msg)) $
    error$ "Failed to write message in one go, length: "++
      show (BSC.length msg)++"cnt: "++ show cnt
  return ()

-- TODO: Handle errors
readFd :: MVar TransportState -> FilePath -> Fd -> MVar () -> IO Transport
readFd state filename fd lock = do
  takeMVar lock
  hdr <- spinread 8 -- read the header (endpoint ID, cmd)
  let addr = makeEndpoint filename $ decodeInt32 (BSC.take 4 hdr)
  chan <- modifyMVar state $ \st -> return (st, lookupEndPoint st addr)
  let cmd = (decodeInt32 . BSC.take 4 . BSC.drop 4) hdr
  sz <- spinread 4
  remoteaddr <- liftM EndPointAddress $ spinread (decodeInt32 sz)
  case (tryToEnum cmd) of
    Just NewConnection -> do
      connId <- modifyMVar state $ \st -> do
        let (cid, cst) = addConnection st (addr,remoteaddr)
        return (cst, cid)
      writeChan chan $ ConnectionOpened connId ReliableOrdered remoteaddr
      putMVar lock ()
      readFd state filename fd lock
    Just CloseConnection -> do
      connId <- modifyMVar state $ \st -> do
        let cid = lookupConnection st (addr,remoteaddr)
        return (removeConnection st (addr,remoteaddr), cid)
      writeChan chan $ ConnectionClosed connId
      putMVar lock ()
      readFd state filename fd lock
    Just Data -> do
      connId <- modifyMVar state $ \st ->
        return (st, lookupConnection st (addr,remoteaddr))
      msz <- spinread 4
      msg <- spinread (decodeInt32 msz)
      writeChan chan $ Received connId [msg]
      putMVar lock ()
      readFd state filename fd lock
    Nothing -> throwIO $ userError "Invalid control request" 
  where
    spinread :: Int -> IO ByteString
    spinread desired = do
      msg <- tryUntilNoIOErr$ PIO.fdRead fd (fromIntegral desired)
      case BSC.length msg of
        n | n == desired -> return msg
        0 -> do threadDelay (10*1000)
                spinread desired
        l -> error$ "Incomplete read expected either 0 bytes or complete msg ("++
             show desired ++" bytes) got "++ show l ++ " bytes"

------------------------------------------------------------------------------

-- | Create a new Transport.
--
createTransport :: IO (Either IOException Transport)
createTransport = do
  state <- newMVar State { endpoints   = Map.empty
                         , connections = Map.empty
                         }
  uid <- randomIO :: IO Word64
  let filename = "/tmp/pipe_"++show uid
  createNamedPipe filename $ unionFileModes ownerReadMode ownerWriteMode
  fd <- PIO.openFd filename PIO.ReadOnly Nothing fileFlags
  lock <- newMVar ()
  void $ forkOS $ void . tryIO . asyncWhenCancelled closeTransport $
    readFd state filename fd lock
  tryIO $ return Transport { newEndPoint    = apiNewEndPoint state filename
                           , closeTransport = apiCloseTransport state filename fd
                           }

-- | Close the transport
apiCloseTransport :: MVar TransportState -> FilePath -> Fd -> IO ()
apiCloseTransport state filename fd = do
  st <- readMVar state
  void . tryIO . asyncWhenCancelled return $ do
    -- TODO: close all connections.
    zipWithM_ apiCloseEndPoint (Map.keys $ endpoints st) (repeat state)
    PIO.closeFd fd
    removeFile filename
    modifyMVar_ state $ \_ -> return State { endpoints   = Map.empty
                                           , connections = Map.empty
                                           }

-- | Create a new endpoint
apiNewEndPoint :: MVar TransportState -> FilePath
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint state filename = do
  chan <- newChan
  addr <- modifyMVar state $ \st -> do
    let addr = makeEndpoint filename $ Map.size (endpoints st)
    return (addEndPoint st addr chan, addr)
  return . Right $ EndPoint { receive       = readChan chan
                            , address       = addr
                            , connect       = apiConnect addr state
                            , closeEndPoint = apiCloseEndPoint addr state
                            , newMulticastGroup = return . Left $ newMulticastGroupError
                            , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
                            }
  where
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

-- | Force-close the endpoint
apiCloseEndPoint :: EndPointAddress -> MVar TransportState -> IO ()
apiCloseEndPoint addr state =
  asyncWhenCancelled return $ do
    chan <- modifyMVar state $ \st -> do
      let ch = lookupEndPoint st addr
      return (removeEndPoint st addr, ch)
    writeChan chan $ EndPointClosed

-- | Create a new connection
apiConnect :: EndPointAddress -> MVar TransportState -> EndPointAddress
           -> Reliability -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect myAddress state theirAddress _reliability _hints = do
  let (path, endId) = splitEndpoint theirAddress
  connAlive <- newMVar True
  if (fst . splitEndpoint) myAddress == path
    then connectToSelf (myAddress,theirAddress) state connAlive
    else do
    fd <- PIO.openFd path PIO.WriteOnly Nothing fileFlags
    writeFd fd $ [encodeInt32 endId,
                  encodeInt32 NewConnection,
                  encodeAddress myAddress]
    return . Right $ Connection { send  = apiSend fd endId myAddress connAlive
                                , close = apiClose fd endId myAddress connAlive
                                }

-- May throw a TransportError ConnectErrorCode (if the local endpoint is closed)
connectToSelf :: EndPointPair -> MVar TransportState -> MVar Bool
              -> IO (Either (TransportError ConnectErrorCode) Connection)
connectToSelf ep@(myAddress,theirAddress) state connAlive = do
  (connId, chan) <- modifyMVar state $ \st -> do
    let (cid, cst) = addConnection st ep
    return (cst, (cid, lookupEndPoint st theirAddress))
  writeChan chan $ ConnectionOpened (fromIntegral connId) ReliableOrdered myAddress
  return . Right $ Connection
      { send  = selfSend chan connId
      , close = (modifyMVar_ state $ \st ->
                  return $ removeConnection st ep) >> (selfClose chan connId)
      }
  where
    selfSend :: Chan Event -> ConnectionId -> [ByteString]
             -> IO (Either (TransportError SendErrorCode) ())
    selfSend chan connId msg = modifyMVar connAlive $ \alive -> do
      if alive
        then do
        writeChan chan $ Received (fromIntegral connId) msg
        return (alive, Right ())
        else return (alive, Left (TransportError SendClosed "Connection closed"))

    selfClose :: Chan Event -> ConnectionId -> IO ()
    selfClose chan connId = do
      modifyMVar_ connAlive $ \alive -> do
      when alive $ writeChan chan $ ConnectionClosed (fromIntegral connId)
      return False

-- | Send a message over a connection
apiSend :: Fd -> Int -> EndPointAddress -> MVar Bool -> [ByteString]
        -> IO (Either (TransportError SendErrorCode) ())
apiSend fd endId myAddr connAlive msg =
  modifyMVar connAlive $ \alive ->
  if alive
  then do
    let hdr = [encodeInt32 endId, encodeInt32 Data, encodeAddress myAddr]
    writeFd fd $ hdr ++ (prependLength msg)
    return (alive, Right ())
  else
    return (alive, Left (TransportError SendFailed "Connection closed"))

-- | Close a connection
apiClose :: Fd -> Int -> EndPointAddress -> MVar Bool -> IO ()
apiClose fd endId myAddr connAlive = do
  modifyMVar_ connAlive $ \alive -> do
    when alive $ do
      writeFd fd [encodeInt32 endId,
                  encodeInt32 CloseConnection,
                  encodeAddress myAddr]
      PIO.closeFd fd
    return False

-- | Accessors for transport endpoints
--
accEndPoint :: TransportState -> (EndPointMap -> EndPointMap) -> TransportState
accEndPoint st f = State { endpoints   = f (endpoints st)
                         , connections = connections st
                         }

-- | Endpoint Helpers
--
addEndPoint :: TransportState -> EndPointAddress -> Chan Event -> TransportState
addEndPoint st a c = accEndPoint st $ Map.insert a c

removeEndPoint :: TransportState -> EndPointAddress -> TransportState
removeEndPoint st a = accEndPoint st $ Map.delete a

lookupEndPoint :: TransportState -> EndPointAddress -> Chan Event
lookupEndPoint st a = Map.findWithDefault (error "Invalid endpoint") a (endpoints st)

-- Join the endpoint path (path to the named pipe) and endpoint
-- number to make a new endpoint address.
makeEndpoint :: FilePath -> Int -> EndPointAddress
makeEndpoint path endId = EndPointAddress . BSC.pack $ path++":"++(show endId)

-- Split an endpoint address represented by e into its endpoint
-- path (the path to the named pipe) and the endpoint number.
splitEndpoint :: EndPointAddress -> (FilePath, Int)
splitEndpoint (EndPointAddress e) = let (path, num) = BSC.break (== ':') e in
  (BSC.unpack path, read (BSC.unpack $ BSC.drop 1 num) :: Int)

-- Encode an endpoint address as a ByteString prepending it with its length
encodeAddress :: EndPointAddress -> ByteString
encodeAddress x = BSC.append (encodeInt32  $ BSC.length addr) addr
  where addr = endPointAddressToByteString x

-- | Accessors for transport connections
--
accConnection :: TransportState -> (ConnectionMap -> ConnectionMap)
              -> TransportState
accConnection st f = State { endpoints   = endpoints st
                           , connections = f (connections st)
                           }

-- Add a new connection, if it is not already present.
addConnection :: TransportState -> EndPointPair -> (ConnectionId, TransportState)
addConnection st p = (connId, accConnection st $ Map.insert p connId)
  where
    connId = fromIntegral $ Map.size (connections st) + 1

removeConnection :: TransportState -> EndPointPair -> TransportState
removeConnection st p = accConnection st $ Map.delete p

lookupConnection :: TransportState -> EndPointPair -> ConnectionId
lookupConnection st a = Map.findWithDefault (error "Invalid Connection") a
                        (connections st)

