{-# LANGUAGE BangPatterns, CPP, ScopedTypeVariables, PackageImports #-}

-- | Named Pipes implementation of the Transport API.
module Network.Transport.Pipes (createTransport) where

import Control.Monad (when, unless)
import Control.Exception (IOException, handle)
import Control.Concurrent.MVar
import Control.Concurrent.Chan (Chan, newChan, readChan, writeChan)
import Control.Concurrent (threadDelay)
import Data.IORef
import Data.Word
import Data.Map (Map)
import qualified Data.Map as Map (empty, insert, size, delete, findWithDefault)
import qualified Data.Foldable as F
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC (pack, length, append, empty)
import Data.ByteString.Lazy.Char8 (fromStrict, toStrict)
import Data.Binary (Binary, Get, encode, decode, put, get)
import System.Random (randomIO)
import System.Posix.Files (FilePath, createNamedPipe, unionFileModes, ownerReadMode, ownerWriteMode)
import System.Posix.Types (Fd)
import System.Directory (removeFile)
import qualified "unix-bytestring" System.Posix.IO.ByteString as PIO
import System.Posix.IO as PIO (openFd, closeFd, OpenFileFlags(..), OpenMode(ReadOnly, WriteOnly))
import Network.Transport
import Network.Transport.Internal (tryIO, encodeInt32, decodeInt32, asyncWhenCancelled, void)

data TransportState = State { endpoints   :: !(Map EndPointAddress (Chan Event))
                            , connections :: !(Map EndPointAddress ConnectionId)
                            , status      :: !TransportStatus
                            }

data TransportStatus =
    TransportValid
  | TransportClosed

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

------------------------------------------------------------------------------

instance Binary Event where
  put (Received cid msg)          = do put (0 :: Word8)
                                       put cid
                                       put msg
  put (ConnectionClosed cid)      = do put (1 :: Word8)
                                       put cid
  put (ConnectionOpened cid _ ea) = do put (2 :: Word8)
                                       put cid
                                       put ea
  put (ReceivedMulticast _ _)     = do put (3 :: Word8)
  put EndPointClosed              = put (4 :: Word8)
  put (ErrorEvent _)              = put (5 :: Word8)
  get = do t <- get :: Get Word8
           case t of
             0 -> do cid <- get
                     msg <- get
                     return (Received cid msg)
             1 -> do cid <- get
                     return (ConnectionClosed cid)
             2 -> do cid <- get
                     ea <- get
                     return (ConnectionOpened cid ReliableOrdered ea)
             3 -> return (ReceivedMulticast (MulticastAddress BSC.empty) [])
             4 -> return EndPointClosed
             5 -> return (ErrorEvent (TransportError EventTransportFailed ""))

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
writeEvent :: Fd -> Event -> IO ()
writeEvent fd ev = do
  let msg = toStrict $ encode ev
  cnt <- PIO.fdWrite fd $ BSC.append (encodeInt32 $ BSC.length msg) msg
  unless (fromIntegral cnt == (BSC.length msg + 4)) $
    error$ "Failed to write message in one go, length: "++ show (BSC.length msg) ++ "cnt: "++ show cnt
  return ()

-- TODO: Handle errors
readEvent :: Fd -> MVar () -> IO Event
readEvent fd lock = do
  takeMVar lock
  hdr <- spinread fd 4
  msg <- spinread fd (decodeInt32 hdr)
  putMVar lock ()
  let ev = (decode $ fromStrict msg)
  putStrLn $ "Received:"++(show ev)
  return ev
  where
    spinread :: Fd -> Int -> IO ByteString
    spinread fd desired = do
      msg <- tryUntilNoIOErr$ PIO.fdRead fd (fromIntegral desired)
      case BSC.length msg of
        n | n == desired -> return msg
        0 -> do threadDelay (10*1000)
                spinread fd desired
        l -> error$ "Inclomplete read expected either 0 bytes or complete msg ("++
             show desired ++" bytes) got "++ show l ++ " bytes"

------------------------------------------------------------------------------

-- | Create a new Transport.
--
createTransport :: IO (Either IOException Transport)
createTransport = do
  state <- newMVar State { endpoints = Map.empty
                         , connections = Map.empty
                         , status = TransportValid
                         }
  uid <- randomIO :: IO Word64
  let filename = "/tmp/pipe_"++show uid
  createNamedPipe filename $ unionFileModes ownerReadMode ownerWriteMode
  fd <- PIO.openFd filename PIO.ReadOnly Nothing fileFlags
  lock <- newMVar ()
  try . asyncWhenCancelled closeTransport $ readEvent fd lock
  tryIO $ return Transport { newEndPoint    = apiNewEndPoint state filename
                           , closeTransport = apiCloseTransport state filename fd
                           }

-- | Close the transport
apiCloseTransport :: MVar TransportState -> FilePath -> Fd -> IO ()
apiCloseTransport state pipe fd = do
  st <- readMVar state
  void . tryIO . asyncWhenCancelled return $ do
    case (status st) of
      TransportValid  -> do
        -- Close open connections
        F.mapM_ close (connections st)
        -- Close local endpoints
        F.mapM_ closeEndPoint (endpoints st)
        PIO.closeFd fd
        removeFile pipe
        modifyMVar_ state $
          \_ -> return State { endpoints = Map.empty
                             , connections = Map.empty
                             , status = TransportClosed
                             }
      TransportClosed -> return ()

-- | Create a new endpoint
apiNewEndPoint :: MVar TransportState -> FilePath
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint state pipe = do
  chan <- newChan
  addr <- modifyMVar state $ \st -> do
    let addr = EndPointAddress . BSC.pack $ (show pipe)++(show $ Map.size (endpoints st))
    return $ insertEndPoint st addr chan
  return . Right $ EndPoint { receive       = readChan chan
                            , address       = addr
                            , connect       = apiConnect addr state
                            , closeEndPoint = apiCloseEndPoint addr state
                            , newMulticastGroup     = return . Left $ newMulticastGroupError
                            , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
                            }
  where
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

-- | Force-close the endpoint
apiCloseEndPoint :: EndpointAddress -> MVar TransportState -> IO ()
apiCloseEndPoint addr state =
  asyncWhenCancelled return $ do
    chan <- modifyMVar_ state $ \st -> do
      let ch = lookupEndPoint st addr
      return (removeEndPoint st addr, ch)
    writeChan chan $ EndPointClosed

-- | Create a new connection
apiConnect :: EndPointAddress
           -> MVar TransportState
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect myAddress state theirAddress _reliability _hints = do
  fd <- PIO.openFd (show theirAddress) PIO.WriteOnly Nothing fileFlags
  connAlive <- newMVar True
  let conn = Connection { send  = apiSend fd connAlive
                        , close = apiClose fd connAlive
                        }
  modifyMVar_ state $
    \st -> return (State { endpoints = endpoints st
                         , connections = conn <| (connections st)
                         , status = status st
                         })
  writeEvent fd $ ConnectionOpened (fromIntegral fd) ReliableOrdered myAddress
  return . Right $ conn

-- | Send a message over a connection
apiSend :: Fd -> MVar Bool -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend fd connAlive msg =
  modifyMVar connAlive $ \alive ->
    if alive
      then do
        writeEvent fd $ Received (fromIntegral fd) msg
        return (alive, Right ())
      else
        return (alive, Left (TransportError SendFailed "Connection closed"))

-- | Close a connection
apiClose :: Fd -> MVar Bool -> IO ()
apiClose fd connAlive =
  modifyMVar_ connAlive $ \alive -> do
    when alive $ do
      writeEvent fd $ ConnectionClosed (fromIntegral fd)
      PIO.closeFd fd
    return False

-- | Endpoint accessors for the transport state
--
accEndPoint :: TransportState -> (Map EndPointAddress (Chan Event) -> Map EndPointAddress (Chan Event)) -> TransportState
accEndPoint state f = State { endpoints = f (endpoints st)
                            , connections = connections st
                            , status = status st
                            }

insertEndPoint :: TransportState -> EndpointAddress -> Chan -> TransportState
inserEndPoint state = accEndPoint state $ Map.insert

removeEndPoint :: TransportState -> EndpointAddress -> TransportState
removeEndPoint state = accEndPoint state $ Map.delete

lookupEndPoint :: TransportState -> EndpointAddress -> Chan
lookupEndPoint state = accEndPoint state $ Map.findWithDefault (error "Invalid endpoint")
