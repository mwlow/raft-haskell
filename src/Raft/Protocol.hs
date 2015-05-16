{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}

--Notes: use txt file as state-machine? Therefore we can write a second program
--to visualize the log files/txt files in real time.
--Handle rewriting pids to registry (especially when first starting up)
module Raft.Protocol 
( initRaft
)
where

import System.Environment
import System.Console.ANSI
import System.Random.MWC
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
import Data.List
import qualified Data.Set as Set
import GHC.Generics (Generic)

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

-- | Data structure for appendEntriesRPC. Is serializable.
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

-- | Data structure for the response to appendEntriesRPC. Is serializable.
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

-- | Data structure for requestVoteRPC. Is serializable.
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

-- | Data structure for the response to requestVoteRPC. Is serializable.
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
data RpcMessage a
    = RpcString String                  
    | RpcWhereIsReply WhereIsReply      
    | RpcMonitorRef MonitorRef        
    | RpcProcessMonitorNotification ProcessMonitorNotification
    | RpcAppendEntriesMsg (AppendEntriesMsg a)
    | RpcAppendEntriesResponseMsg AppendEntriesResponseMsg
    | RpcRequestVoteMsg RequestVoteMsg
    | RpcRequestVoteResponseMsg RequestVoteResponseMsg

-- | Echo handling for testing.
echo :: (ProcessId, String) -> Process ()
echo (sender, msg) = do
    self <- getSelfPid
    say $ show self ++ " recv " ++ show sender ++ ": " ++ msg
    send sender msg

logMessage :: String -> Process String
logMessage msg = return msg

-- | These wrapping functions simply return the message in an RPCMsg type.
recvWhereIsReply :: WhereIsReply -> Process (RpcMessage a)
recvWhereIsReply a = return $ RpcWhereIsReply a

recvMonitorRef :: MonitorRef -> Process (RpcMessage a)
recvMonitorRef a = return $ RpcMonitorRef a


-- | This is the process's local view of the entire cluster
data ClusterState = ClusterState 
    { backend        :: Backend      -- ^ Backend for the topology
    , nodes          :: [LocalNode]  -- ^ List of nodes in the topology
    , nodeIds        :: Set.Set NodeId   -- ^ Set of nodeIds
    , activeNodeIds  :: Set.Set NodeId   -- ^ Nodes with an active process
    }

-- | This is the node's local state.
data NodeState = NodeState
    { gen         :: GenIO
    }

-- | This is the main process for handling Raft RPCs.
raftLoop :: ClusterState -> NodeState -> Process ()
raftLoop clusterState@(ClusterState backend nodes nodeIds activeNodeIds) 
         nodeState@(NodeState gen) = do

    -- Randomly send out probe to discover processes
    let inactiveNodeIds = nodeIds Set.\\ activeNodeIds
    rand <- liftIO $ (uniformR (0, Set.size inactiveNodeIds - 1) gen :: IO Int)
    when (Set.size inactiveNodeIds > 0) $ 
        whereisRemoteAsync (Set.elemAt rand inactiveNodeIds) "server"
    
    -- Handle messages. Use timeout to ensure frequent probing.
    msg <- receiveTimeout 250000 [ match recvWhereIsReply
                                 , match recvMonitorRef
                                 ]

    case msg of
        Nothing -> raftLoop clusterState nodeState
        Just (RpcWhereIsReply m) -> handleWhereIsReply m 
        Just (RpcProcessMonitorNotification m) -> handleProcessMonitorNotification m 
  where
    -- If we get WhereIsReply, monitor the process
    handleWhereIsReply :: WhereIsReply -> Process ()
    handleWhereIsReply (WhereIsReply _ (Just pid)) = do
        let newActiveNodeIds = Set.insert (processNodeId pid) activeNodeIds
            newClusterState = ClusterState backend nodes nodeIds newActiveNodeIds
        monitor pid
        say "whereis received~"
        raftLoop newClusterState nodeState
    handleWhereIsReply _ = raftLoop clusterState nodeState

    -- If we get a monitor notification, process has become unreachable
    handleProcessMonitorNotification :: ProcessMonitorNotification -> Process ()
    handleProcessMonitorNotification (ProcessMonitorNotification r p _) = do
        let newActiveNodeIds = Set.delete (processNodeId p) activeNodeIds
            newClusterState = ClusterState backend nodes nodeIds newActiveNodeIds
        unmonitor r
        reconnect p
        say "monitor received!"
        raftLoop newClusterState nodeState


-- | Starts up Raft on the node.
initRaft :: Backend -> [LocalNode] -> Process ()
initRaft backend nodes = do
    -- Hello world!
    say "Yo, I'm alive!"

    -- Create random generator for random events 
    gen <- liftIO createSystemRandom
    let nodeIds = Set.fromList $ localNodeId <$> nodes
        clusterState = ClusterState backend nodes nodeIds Set.empty
        nodeState = NodeState gen
    
    -- Start process for handling messages and register it
    serverPid <- spawnLocal (raftLoop clusterState nodeState)
    register "server" serverPid
    
    --mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
    --mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
--    mapM_ (\x -> nsendRemote x "server" "abcd") (localNodeId <$> nodes)

    return ()
    -- start process to handle messages from outside world.
    -- Use forward to forward all messages to the leader.
