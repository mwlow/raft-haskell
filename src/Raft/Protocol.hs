{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Raft.Protocol 
( initRaft
)
where

import System.Environment
import System.Console.ANSI
import System.Exit
import System.Posix.Signals
import Control.Concurrent 
import Control.Monad 
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Text.Printf
import Data.Binary
import Data.Typeable
import GHC.Generics (Generic)
import qualified Data.Map as Map

-- | Type renaming to make things clearer.
type Term = Int
type Index = Int
type ServerId = Int
type Log a = [LogEntry a]

data ServerRole = Follower | Candidate | Leader deriving (Show, Read, Eq)

-- | Wrapper around state transition rule. Derive Typeable and Binary
-- so that we can serialize it.
newtype Command a = Command a deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (Command a) where
    put (Command a) = put a
    get = get >>= \a -> return (Command a)

-- | An entry in a log, clearly. Also serializable.
data LogEntry a = LogEntry 
    { index          :: Index
    , termReceived   :: Term
    , committed      :: Bool
    , command        :: Command a
    } deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (LogEntry a) where
    put (LogEntry a b c d) = put a >> put b >> put c >> put d
    get = do
        a <- get
        b <- get
        c <- get
        d <- get
        return $ LogEntry a b c d

-- 
data AppendEntriesMsg a = AppendEntriesMsg 
    { aeSender       :: ProcessId -- ^ Sender's process id
    , aeSeqno        :: Int       -- ^ Sequence number, for matching replies
    , aeTerm         :: Term      -- ^ Leader's term
    , leaderId       :: NodeId    -- ^ Leader's Id
    , prevLogIndex   :: Index     -- ^ Log entry index right before new ones
    , prevLogTerm    :: Term      -- ^ Term of the previous log entry
    , entries        :: Log a     -- ^ Log entries to store, [] for heartbeat
    , leaderCommit   :: Index     -- ^ Leader's commit index
    } deriving (Show, Eq, Typeable)

instance (Binary a) => Binary (AppendEntriesMsg a) where
    put (AppendEntriesMsg a b c d e f g i) =
        put a >> put b >> put c >> put d >> put e >> put f >> put g >> put i
    get = do
        a <- get
        b <- get
        c <- get
        d <- get
        e <- get
        f <- get
        g <- get
        i <- get
        return $ AppendEntriesMsg a b c d e f g i

data AppendEntriesResponseMsg = AppendEntriesResponseMsg 
    { aerSender   :: ProcessId  -- ^ Sender's process id
    , aerSeqno    :: Int        -- ^ Sequence number, for matching replies
    , aerTerm     :: Term       -- ^ Current term to update leader
    , success     :: Bool       -- ^ Did follower contain a matching entry?
    } deriving (Show, Eq, Typeable)

instance Binary AppendEntriesResponseMsg where
    put (AppendEntriesResponseMsg a b c d) = put a >> put b >> put c >> put d
    get = do
        a <- get
        b <- get
        c <- get
        d <- get
        return $ AppendEntriesResponseMsg a b c d

data RequestVoteMsg = RequestVoteMsg
    { rvSender      :: ProcessId  -- ^ Sender's process id
    , rvSeqno       :: Int        -- ^ Sequence number, for matching replies
    , rvTerm        :: Term       -- ^ Candidate's term
    , candidateId   :: NodeId     -- ^ Candidate requesting vote
    , lastLogIndex  :: Index      -- ^ Index of candidate's last log entry
    , lastLogTerm   :: Term       -- ^ Term of candidate's last log entry
    } deriving (Show, Eq, Typeable)

instance Binary RequestVoteMsg where
    put (RequestVoteMsg a b c d e f) = 
        put a >> put b >> put c >> put d >> put e >> put f
    get = do
        a <- get
        b <- get
        c <- get
        d <- get
        e <- get
        f <- get
        return $ RequestVoteMsg a b c d e f

data RequestVoteResponseMsg = RequestVoteResponseMsg
    { rvrSender     :: ProcessId  -- ^ Sender's process id
    , rvrSeqno      :: Int        -- ^ Sequence number, for matching replies
    , rvrTerm       :: Term       -- ^ Current term to update leader
    , voteGranted   :: Bool       -- ^ Did candidate receive vote?
    } deriving (Show, Eq, Typeable)

instance Binary RequestVoteResponseMsg where
    put (RequestVoteResponseMsg a b c d) =
        put a >> put b >> put c >> put d
    get = do
        a <- get
        b <- get
        c <- get
        d <- get
        return $ RequestVoteResponseMsg a b c d

-- | Hack to allow us to process arbitrary messages from the mailbox
data RpcMessage = RPCString String | RPCInt Int

-- | Handle whereisRemoteAsync queries. Raft node failures will be 
-- simulated by processes that shut down and don't respond. Returns a monitor.
whereisRemoteAsyncHandler :: WhereIsReply -> Process (Maybe MonitorRef) 
whereisRemoteAsyncHandler (WhereIsReply _ (Just pid)) = 
    monitor pid >>= \m -> return (Just m)
whereisRemoteAsyncHandler _ = return Nothing

-- | Echo handling for testing.
echo :: (ProcessId, String) -> Process ()
echo (sender, msg) = do
    self <- getSelfPid
    say $ show self ++ " recv " ++ show sender ++ ": " ++ msg
    send sender msg

logMessage :: String -> Process ()
logMessage msg = say $ "Handling: " ++ msg



-- | This is the main process for handling Raft RPCs.
raftProcess :: Backend -> [LocalNode] -> Process ()
raftProcess backend nodes = do
    -- Let all Raft instances start up
    liftIO $ threadDelay 500000

    -- Initialize state
    let monitors = Map.empty

    -- Query for processIds
    --mapM_ (`whereisRemoteAsync` "server") $ localNodeId <$> nodes

    -- Handle messages
    forever $ do
        ref <- receiveWait []
        return ()

-- | Starts up Raft on the node.
initRaft :: Backend                -- ^ Backend that the cluster is running on
         -> [LocalNode]            -- ^ Nodes on the topology
         -> Process ()
initRaft backend nodes = do
    -- Hello world!
    say "Yo, I'm alive!"
    
    -- Start process for handling messages and register it
    serverPid <- spawnLocal (raftProcess backend nodes)
    register "server" serverPid
    
    --mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
    --mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
    mapM_ (\x -> nsendRemote x "server" ([Command "hi"])) (localNodeId <$> nodes)

    return ()
    -- start process to handle messages from outside world.
    -- Use forward to forward all messages to the leader.
