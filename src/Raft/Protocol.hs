{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}

--Notes: use txt file as state-machine? Therefore we can write a second program
--to visualize the log files/txt files in real time.
--Handle rewriting pids to registry (especially when first starting up)
-- start process to handle messages from outside world.
-- Use forward to forward all messages to the leader.

--TODO: It looks like we have to spawn a new thread to handle votes,
-- i.e. voteserver
module Raft.Protocol 
( initRaft
)
where

import Prelude hiding (log)
import System.Environment
import System.Console.ANSI
import System.Random.MWC
import System.Exit
import System.Posix.Signals
import Control.Concurrent 
import Control.Monad
import qualified Control.Lens as Lens
import Control.Lens.Operators
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Extras (isProcessAlive)
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Text.Printf
import Data.Binary
import Data.Typeable
import Data.List
import qualified Data.Set as Set
import qualified Data.Map as Map
import GHC.Generics (Generic)

-- | Type renaming to make things clearer.
type Term = Int
type Index = Int
type Log a = [LogEntry a]

data ServerRole = Follower | Candidate | Leader deriving (Show, Read, Eq)

-- | Wrapper around state transition rule. Derive Typeable and Binary
-- so that we can serialize it.
newtype Command a = Command a deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (Command a) where
    put (Command a) = put a
    get = Command <$> get

-- | An entry in a log, clearly. Also serializable.
data LogEntry a = LogEntry 
    { index          :: Index
    , termReceived   :: Term
    , committed      :: Bool
    , command        :: Command a
    } deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (LogEntry a) where
    put (LogEntry a b c d) = put a >> put b >> put c >> put d
    get = LogEntry <$> get <*> get <*> get <*> get 

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
    get = AppendEntriesMsg <$> get <*> get <*> get <*> get <*> get <*> get
                           <*> get <*> get

-- | Data structure for the response to appendEntriesRPC. Is serializable.
data AppendEntriesResponseMsg = AppendEntriesResponseMsg 
    { aerSender   :: ProcessId  -- ^ Sender's process id
    , aerSeqno    :: Int        -- ^ Sequence number, for matching replies
    , aerTerm     :: Term       -- ^ Current term to update leader
    , success     :: Bool       -- ^ Did follower contain a matching entry?
    } deriving (Show, Eq, Typeable)

instance Binary AppendEntriesResponseMsg where
    put (AppendEntriesResponseMsg a b c d) = put a >> put b >> put c >> put d
    get = AppendEntriesResponseMsg <$> get <*> get <*> get <*> get

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
    get = RequestVoteMsg <$> get <*> get <*> get <*> get <*> get <*> get

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
    get = RequestVoteResponseMsg <$> get <*> get <*> get <*> get

-- | Hack to allow us to process arbitrary messages from the mailbox
data RpcMessage a
    = RpcNothing                
    | RpcWhereIsReply WhereIsReply           
    | RpcProcessMonitorNotification ProcessMonitorNotification
    | RpcAppendEntriesMsg (AppendEntriesMsg a)
    | RpcAppendEntriesResponseMsg AppendEntriesResponseMsg
    | RpcRequestVoteMsg RequestVoteMsg
    | RpcRequestVoteResponseMsg RequestVoteResponseMsg

-- | These wrapping functions return the message in an RPCMsg type.
recvSay :: String -> Process (RpcMessage a)
recvSay a = say a >> return RpcNothing

recvEcho :: (ProcessId, String) -> Process (RpcMessage a)
recvEcho (pid, a) = send pid a >> return RpcNothing

recvWhereIsReply :: WhereIsReply -> Process (RpcMessage a)
recvWhereIsReply a =  return $ RpcWhereIsReply a

recvProcessMonitorNotification :: ProcessMonitorNotification 
                               -> Process (RpcMessage a)
recvProcessMonitorNotification a = return $ RpcProcessMonitorNotification a

recvAppendEntriesMsg :: AppendEntriesMsg a -> Process (RpcMessage a)
recvAppendEntriesMsg a = return $ RpcAppendEntriesMsg a

recvAppendEntriesResponseMsg :: AppendEntriesResponseMsg 
                             -> Process (RpcMessage a)
recvAppendEntriesResponseMsg a = return $ RpcAppendEntriesResponseMsg a

recvRequestVoteMsg :: RequestVoteMsg -> Process (RpcMessage a)
recvRequestVoteMsg a = return $ RpcRequestVoteMsg a

recvRequestVoteResponseMsg :: RequestVoteResponseMsg -> Process (RpcMessage a)
recvRequestVoteResponseMsg a = return $ RpcRequestVoteResponseMsg a


-- | This is the process's local view of the entire cluster.
data ClusterState = ClusterState 
    { _backend    :: Backend                  -- ^ Backend for the topology
    , _nodeIds    :: [NodeId]                 -- ^ List of nodeIds in the topology
    , _knownIds   :: Map.Map NodeId ProcessId -- ^ Map of known processIds
    , _unknownIds :: Map.Map NodeId ProcessId -- ^ Map of unknown processIds
    }
Lens.makeLenses ''ClusterState -- Lens'd for easy get and set

-- | This is the node's local state.
data NodeState a = NodeState
    { _mainPid      :: ProcessId     -- ^ ProcessId of the thread running Raft
    , _randomGen    :: GenIO         -- ^ Random generator for random events

    , _state        :: ServerRole
    , _currentTerm  :: Term
    , _votedFor     :: Maybe NodeId
    , _log          :: Log a
    , _commitIndex  :: Index
    , _lastApplied  :: Index

    , _nextIndex    :: Map.Map NodeId Index
    , _matchIndex   :: Map.Map NodeId Index

    , _voteCount    :: Int
    }
Lens.makeLenses ''NodeState -- Lens'd for easy get and set

-- | Helper Functions
stepDown :: NodeState a -> Term -> NodeState a
stepDown nodeState newTerm = n3
  where 
    n1 = currentTerm .~ newTerm $ nodeState
    n2 = state .~ Follower $ n1
    n3 = votedFor .~ Nothing $ n2

logTerm :: NodeState a -> Index -> Term
logTerm nodeState index = termReceived $ (nodeState ^. log) !! index

-- | Handle Request Vote request from peer
handleRequestVoteMsg :: ClusterState 
                     -> NodeState a 
                     -> RequestVoteMsg
                     -> Process (ClusterState, NodeState a)
handleRequestVoteMsg 
    clusterState
    nodeState
    msg@(RequestVoteMsg sender seqno term candidateId lastLogIndex lastLogTerm)
    | nCurrentTerm < term = do
        let n0 = stepDown nodeState term
        reply term False
        return (clusterState, n0)
    | nCurrentTerm == term &&
      (nVotedFor == Nothing || nVotedFor == Just candidateId) &&
      (lastLogTerm > nLastLogTerm ||
       (lastLogTerm == nLastLogTerm && 
        lastLogIndex >= length nLog)) = do
        let n0 = votedFor .~ Just candidateId $ nodeState
        reply term True
        return (clusterState, n0)
    | otherwise = do
        reply term False
        return (clusterState, nodeState)
  where
    nLog = nodeState ^. log
    nVotedFor = nodeState ^. votedFor
    nCurrentTerm = nodeState ^. currentTerm
    nLastLogTerm = termReceived (last nLog)
    reply :: Term -> Bool -> Process ()
    reply term granted = do
        self <- getSelfPid
        send sender (RequestVoteResponseMsg self seqno term granted)

-- | Handle Request Vote response from peer
handleRequestVoteResponseMsg :: ClusterState
                             -> NodeState a
                             -> RequestVoteResponseMsg
                             -> Process (ClusterState, NodeState a)
handleRequestVoteResponseMsg 
    clusterState
    nodeState
    msg@(RequestVoteResponseMsg sender seqno term granted) =
        case granted of
            True -> 
                if nodeState ^. currentTerm == term
                    then return (clusterState, nodeState & voteCount %~ (+1))
                    else return (clusterState, nodeState)
            False ->
                return (clusterState, nodeState)


initRaft :: Backend -> [LocalNode] -> Process ()
initRaft backend nodes = do return ()


{-


-- | Drain and process all control messages in the mailbox. The reason that
-- this is handled on the same thread as the main loop is that we need to
-- call reconnect from the pid of the main loop.
-- DANGER: Should not cause any new ctrl messages to be received--infinite loop!
handleCtrlMessages :: ClusterState -> NodeState 
                   -> Process (ClusterState, NodeState)
handleCtrlMessages
    clusterState@(ClusterState backend nodeIds knownIds unknownIds)
    nodeState@(NodeState mainPid randomGen) = do
    
    msg <- receiveTimeout 0 [ match recvSay
                            , match recvEcho
                            , match recvProcessMonitorNotification
                            , match recvWhereIsReply
                            ]

    -- Pass control message on to appropriate handler
    case msg of
        Nothing -> return (clusterState, nodeState)
        Just RpcNothing 
                -> handleCtrlMessages clusterState nodeState
        Just (RpcWhereIsReply m) 
                -> handleWhereIsReply m 
        Just (RpcProcessMonitorNotification m) 
                -> handleProcessMonitorNotification m 
        other@_ -> return (clusterState, nodeState)

  where
    -- If we get WhereIsReply, monitor the process for disconnect
    handleWhereIsReply :: WhereIsReply -> Process (ClusterState, NodeState)
    handleWhereIsReply (WhereIsReply _ (Just pid)) = do
        -- Determine whether pid already exists in the knownIds
        case Map.lookup (processNodeId pid) knownIds of
            Just pid -> handleCtrlMessages clusterState nodeState
            other@_  -> return (clusterState, nodeState) -- dummy
        -- New pid found, insert it in map and monitor
        let newKnownIds = Map.insert (processNodeId pid) pid knownIds
            newUnknownIds = Map.delete (processNodeId pid) unknownIds
            newClusterState = ClusterState backend nodeIds newKnownIds newUnknownIds
        monitor pid -- start monitoring
        handleCtrlMessages newClusterState nodeState
    handleWhereIsReply _ = handleCtrlMessages clusterState nodeState

    -- If we get a monitor notification, process has become unreachable
    handleProcessMonitorNotification :: ProcessMonitorNotification 
                                     -> Process (ClusterState, NodeState)
    handleProcessMonitorNotification (ProcessMonitorNotification r p _) = do
        let newKnownIds = Map.delete (processNodeId p) knownIds
            newUnknownIds = Map.insert (processNodeId p) p unknownIds
            newClusterState = ClusterState backend nodeIds newKnownIds newUnknownIds
        unmonitor r -- remove monitor
        reconnect p -- set reconnect flag
        handleCtrlMessages newClusterState nodeState


-- | This is the main process for handling Raft RPCs.
raftLoop :: ClusterState -> NodeState -> Process ()
raftLoop clusterState@(ClusterState backend nodeIds knownIds unknownIds) 
         nodeState@(NodeState mainPid randomGen) = do

    -- Die when parent does
    link mainPid

    -- Randomly send out probe to discover processes
    unless (Map.null unknownIds) $ do
        rand <- liftIO (uniformR (0, Map.size unknownIds - 1) randomGen :: IO Int)
        whereisRemoteAsync (fst $ Map.elemAt rand unknownIds) "server"

    -- Choose election timeout between 150ms and 300ms
    timeout <- liftIO (uniformR (150000, 300000) randomGen :: IO Int)

    -- Wait for raft messages
    msg <- receiveTimeout timeout []

    -- Check for control messages
    -- TODO: instead of clusterState/nodeState, feed in state that has been
    -- modified by the election message handler
    (newClusterState, newNodeState) <- handleCtrlMessages clusterState nodeState
    
    -- Repeat forever
    raftLoop newClusterState newNodeState


-- | Starts up Raft on the node.
initRaft :: Backend -> [LocalNode] -> Process ()
initRaft backend nodes = do
    -- Hello world!
    selfPid <- getSelfPid
    say "Yo, I'm alive!"

    -- Initialize state
    randomGen <- liftIO createSystemRandom
    let nodeIds = localNodeId <$> nodes
        knownIds = Map.empty
        unknownIds = Map.fromList . zip nodeIds . repeat $ selfPid
        clusterState = ClusterState backend nodeIds knownIds unknownIds
        nodeState = NodeState selfPid randomGen
    
    -- Start process for handling messages and register it
    serverPid <- spawnLocal (raftLoop clusterState nodeState)
    reg <- whereis "server"
    case reg of
        Nothing -> register "server" serverPid
        other@_ -> reregister "server" serverPid

    -- Kill this process if server dies
    link serverPid

    mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)

    -- Hack to block
    x <- receiveWait []
    return ()


-}
