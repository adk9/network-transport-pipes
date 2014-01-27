{-# LANGUAGE BangPatterns, CPP, ScopedTypeVariables, PackageImports #-}

-- | Named Pipes implementation of the Transport API.
module Network.Transport.Pipes (createTransport) where

import Control.Monad (when, unless)
import Control.Exception (IOException, handle)
import Control.Concurrent.MVar
import Control.Concurrent (threadDelay)
import Data.IORef
import Data.Word
import Data.Sequence (Seq, (<|))
import qualified Data.Sequence as Seq
import qualified Data.Foldable as F
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC (pack, length, append, empty)
import Data.ByteString.Lazy.Char8 (fromStrict, toStrict)
import Data.Binary (Binary, Get, encode, decode, put, get)
import System.Random (randomIO)
import System.Posix.Files (createNamedPipe, unionFileModes, ownerReadMode, ownerWriteMode)
import System.Posix.Types (Fd)
import System.Directory (removeFile)
import qualified "unix-bytestring" System.Posix.IO.ByteString as PIO
import System.Posix.IO as PIO (openFd, closeFd, OpenFileFlags(..), OpenMode(ReadOnly, WriteOnly))
import Network.Transport
import Network.Transport.Internal (tryIO, encodeInt32, decodeInt32, asyncWhenCancelled)

data TransportState = State { endpoints   :: !(Seq EndPoint)
                            , connections :: !(Seq Connection)
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
  state <- newMVar State { endpoints = Seq.empty
                         , connections = Seq.empty
                         , status = TransportValid
                         }
  tryIO $ return Transport { newEndPoint    = apiNewEndPoint state
                           , closeTransport = apiCloseTransport state
                           }

-- | Create a new endpoint
apiNewEndPoint :: MVar TransportState -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint state = do
  uid <- randomIO :: IO Word64
  let filename = "/tmp/pipe_"++show uid
  createNamedPipe filename $ unionFileModes ownerReadMode ownerWriteMode
  fd <- PIO.openFd filename PIO.ReadOnly Nothing fileFlags
  lock <- newMVar ()
  let addr = EndPointAddress . BSC.pack $ filename
      endp = EndPoint { receive       = readEvent fd lock
                      , address       = addr
                      , connect       = apiConnect addr state
                      , closeEndPoint = apiCloseEndPoint fd addr
                      , newMulticastGroup     = return . Left $ newMulticastGroupError
                      , resolveMulticastGroup = return . Left . const resolveMulticastGroupError
                      }
  modifyMVar_ state $
    \st -> return (State { endpoints = endp <| (endpoints st)
                         , connections = connections st
                         , status = status st
                         })
  return . Right $ endp
  where
    newMulticastGroupError =
      TransportError NewMulticastGroupUnsupported "Multicast not supported"
    resolveMulticastGroupError =
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported"

-- | Close the transport
apiCloseTransport :: MVar TransportState -> IO ()
apiCloseTransport state = do
  st <- readMVar state
  asyncWhenCancelled return $ do
    case (status st) of
      TransportValid  -> do
        -- Close open connections
        F.mapM_ close (connections st)
        -- Close local endpoints
        F.mapM_ closeEndPoint (endpoints st)
        modifyMVar_ state $
          \_ -> return State { endpoints = Seq.empty
                             , connections = Seq.empty
                             , status = TransportClosed
                             }
      TransportClosed -> return ()

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

-- | Force-close the endpoint
apiCloseEndPoint :: Fd -> EndPointAddress -> IO ()
apiCloseEndPoint fd addr =
  asyncWhenCancelled return $ do
    writeEvent fd $ EndPointClosed
    PIO.closeFd fd
    removeFile $ show addr
