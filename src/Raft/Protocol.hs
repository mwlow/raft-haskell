{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}

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
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Trans
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
import Safe
import qualified Data.Set as Set
import qualified Data.IntMap as IntMap
import qualified Data.Map as Map
import GHC.Generics (Generic)

-- | Type renaming to make things clearer.
type Term = Int
type Index = Int
type Log a = IntMap.IntMap (LogEntry a)

data ServerRole = Follower | Candidate | Leader deriving (Show, Read, Eq)

-- | Wrapper around state transition rule. Derive Typeable and Binary
-- so that we can serialize it.
newtype Command a = Command a deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (Command a) where
    put (Command a) = put a
    get = Command <$> get

-- | An entry in a log, clearly. Also serializable.
data LogEntry a = LogEntry 
    { termReceived   :: Term
    , committed      :: Bool
    , command        :: Command a
    } deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (LogEntry a) where
    put (LogEntry a b c) = put a >> put b >> put c
    get = LogEntry <$> get <*> get <*> get

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
    { aerSender     :: ProcessId  -- ^ Sender's process id
    , aerSeqno      :: Int        -- ^ Sequence number, for matching replies
    , aerTerm       :: Term       -- ^ Current term to update leader
    , aerMatchIndex :: Index      -- ^ Index of highest log entry replicated
    , success       :: Bool       -- ^ Did follower contain a matching entry?
    } deriving (Show, Eq, Typeable)

instance Binary AppendEntriesResponseMsg where
    put (AppendEntriesResponseMsg a b c d e) = 
        put a >> put b >> put c >> put d >> put e
    get = AppendEntriesResponseMsg <$> get <*> get <*> get <*> get <*> get

-- | Data structure for requestVoteRPC. Is serializable.
data RequestVoteMsg = RequestVoteMsg
    { rvSender      :: ProcessId   -- ^ Sender's process id
    , rvSeqno       :: Int         -- ^ Sequence number, for matching replies
    , rvTerm        :: Term        -- ^ Candidate's term
    , candidateId   :: NodeId      -- ^ Candidate requesting vote
    , lastLogIndex  :: Index       -- ^ Index of candidate's last log entry
    , lastLogTerm   :: Term        -- ^ Term of candidate's last log entry
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


-- | This is the process's local view of the entire cluster. Information
-- stored in this data structure should only be modified by the main thread.
data ClusterState = ClusterState 
    { backend     :: Backend                  -- ^ Backend for the topology
    , selfNodeId  :: NodeId                   -- ^ nodeId of current server
    , nodeIds     :: [NodeId]                 -- ^ List of nodeIds in the topology
    , peers       :: [NodeId]                 -- ^ nodeIds \\ [selfNodeId]
    , knownIds    :: Map.Map NodeId ProcessId -- ^ Map of known processIds
    , unknownIds  :: Map.Map NodeId ProcessId -- ^ Map of unknown processIds

    , mainPid     :: ProcessId     -- ^ ProcessId of the master thread
    , raftPid     :: ProcessId     -- ^ ProcessId of the thread running Raft
    , delayPid    :: ProcessId     -- ^ ProcessId of the delay thread
    , randomGen   :: GenIO         -- ^ Random generator for random events
    }


-- | This is state that is volatile and can be modified by multiple threads
-- concurrently. This state will always be passed to the threads in an MVar.
data RaftState a = RaftState
    { leader       :: NodeId                  
    , state        :: ServerRole
    , currentTerm  :: Term
    , votedFor     :: Maybe NodeId
    , log          :: Log a
    , commitIndex  :: Index
    , lastApplied  :: Index

    , nextIndexMap    :: Map.Map NodeId Index
    , matchIndexMap   :: Map.Map NodeId Index

    , voteCount    :: Int
    }


-- | Helper Functions
stepDown :: RaftState a -> Term -> RaftState a
stepDown raftState newTerm =
    raftState { currentTerm = newTerm
              , state       = Follower
              , votedFor    = Nothing
              , voteCount   = 0
              }

logTerm :: Log a -> Index -> Term
logTerm log index
    | index < 1 = 0
    | index > IntMap.size log = 0
    | otherwise = termReceived (log IntMap.! index)

-- | Handle Request Vote request from peer
handleRequestVoteMsg :: ClusterState 
                     -> RaftState a 
                     -> RequestVoteMsg
                     -> Process (ClusterState, RaftState a)
handleRequestVoteMsg clusterState nodeState
    (RequestVoteMsg 
        sender 
        seqno 
        term 
        candidateId 
        lastLogIndex
        lastLogTerm)
    | term > nCurrentTerm =
        reply term False >> return (clusterState, stepDown nodeState term)
    | term < nCurrentTerm =
        reply nCurrentTerm False >> return (clusterState, nodeState)
    | nVotedFor `elem` [Nothing, Just candidateId] && isUpToDate = do
        let n0 = nodeState { votedFor = Just candidateId }
        reply term True >> return (clusterState, n0)
    | otherwise =
        reply nCurrentTerm False >> return (clusterState, nodeState)
  where
    nLog           = log nodeState
    nLogSize       = IntMap.size nLog
    nVotedFor      = votedFor nodeState
    nCurrentTerm   = currentTerm nodeState
    nLastLogTerm   = logTerm (log nodeState) nLogSize

    isUpToDate :: Bool
    isUpToDate = lastLogTerm > nLastLogTerm || 
        (lastLogTerm == nLastLogTerm && lastLogIndex >= nLogSize)

    reply :: Term -> Bool -> Process ()
    reply term granted = do
        self <- getSelfPid
        send sender (RequestVoteResponseMsg self seqno term granted)


-- | Handle Request Vote response from peer
handleRequestVoteResponseMsg :: ClusterState
                             -> RaftState a
                             -> RequestVoteResponseMsg
                             -> Process (ClusterState, RaftState a)
handleRequestVoteResponseMsg clusterState nodeState
    (RequestVoteResponseMsg 
        sender 
        seqno 
        term 
        granted)
    | term > nCurrentTerm =
        return (clusterState, stepDown nodeState term)
    | otherwise =
        case nState of
            Candidate ->
                if nCurrentTerm == term && granted
                    then return (clusterState 
                               , nodeState { voteCount = nVoteCount + 1 })
                    else return (clusterState, nodeState)
            other@_   -> return (clusterState, nodeState)
  where
    nState         = state nodeState
    nCurrentTerm   = currentTerm nodeState
    nVoteCount     = voteCount nodeState


-- | Handle Append Entries request from peer
handleAppendEntriesMsg :: ClusterState
                       -> RaftState a
                       -> AppendEntriesMsg a
                       -> Process (ClusterState, RaftState a)
handleAppendEntriesMsg clusterState nodeState
    (AppendEntriesMsg
        sender
        seqno
        term
        leaderId
        prevLogIndex
        prevLogTerm
        entries
        leaderCommit)
    | term > nCurrentTerm = do
        reply term 0 False
        return (clusterState, stepDown nodeState term)
    | nCurrentTerm > term = do
        reply nCurrentTerm 0 False
        return (clusterState, nodeState)
    | otherwise = do 
        let n0 = nodeState { leader = leaderId, state = Follower }
        if success 
            then do 
                let oldLog = log nodeState
                    newLog = appendLog oldLog entries prevLogIndex
                    index = IntMap.size newLog
                    n0 = nodeState 
                         { log = newLog
                         , commitIndex = min leaderCommit index
                         }
                reply nCurrentTerm index True
                return (clusterState, n0)
            else do
                reply nCurrentTerm 0 False
                return (clusterState, nodeState)
  where
    nLog           = log nodeState
    nLogSize       = IntMap.size nLog
    nVotedFor      = votedFor nodeState
    nCurrentTerm   = currentTerm nodeState
    nLastLogTerm   = logTerm (log nodeState) nLogSize

    success :: Bool
    success = prevLogIndex == 0 ||
                (prevLogIndex <= nLogSize && 
                 logTerm (log nodeState) prevLogIndex == prevLogIndex)

    reply :: Term -> Index -> Bool -> Process ()
    reply term matchIndex granted = do
        self <- getSelfPid
        send sender (AppendEntriesResponseMsg self seqno term matchIndex granted)

    appendLog :: Log a -> Log a -> Index -> Log a
    appendLog dstLog srcLog prevLogIndex
        | IntMap.null srcLog = dstLog
        | otherwise = 
            if logTerm dstLog index /= termReceived headSrcLog
                then appendLog (IntMap.insert index headSrcLog dstLog)
                                tailSrcLog
                                index
                else appendLog dstLog tailSrcLog index
      where
        index = prevLogIndex + 1
        ((_, headSrcLog), tailSrcLog) = IntMap.deleteFindMin srcLog
        -- TODO This is currently O(n)
        initDstLog = IntMap.filterWithKey (\k _ -> k <= index - 1)


-- | Handle Append Entries response from peer
-- TRY THIS OUT GABRIEL
handleAppendEntriesResponseMsg :: ClusterState
                               -> RaftState a
                               -> AppendEntriesResponseMsg
                               -> Process (ClusterState, RaftState a)
handleAppendEntriesResponseMsg clusterState nodeState
    (AppendEntriesResponseMsg
        sender
        seqno
        term
        matchIndex
        success) = 
    return (clusterState, nodeState) -- placeholder


-- 
-- election thread
-- electionTimeout Thread
-- raftLoop: handles receiving rpcs



-- | This thread starts a new election.
electionThread :: ClusterState -> MVar (RaftState a) -> Process ()
electionThread c mr = do
    -- Die when parent does
    link $ mainPid c

    -- Update raftState and create (Maybe RequestVoteMsg)
    msg <- liftIO $ modifyMVarMasked mr $ \r -> do
        if state r == Leader
            then return (r, Nothing)
            else 
                let r0 = r { state          = Candidate 
                           , votedFor       = Just $ selfNodeId c
                           , currentTerm    = 1 + currentTerm r
                           , voteCount      = 1
                           , matchIndexMap  = Map.fromList $ zip (nodeIds c) [0, 0..]
                           , nextIndexMap   = Map.fromList $ zip (nodeIds c) [1, 1..]
                           }
                    msg = RequestVoteMsg 
                           { rvSender     = raftPid c 
                           , rvSeqno      = 0
                           , rvTerm       = currentTerm r0
                           , candidateId  = selfNodeId c
                           , lastLogIndex = IntMap.size $ log r
                           , lastLogTerm  = logTerm (log r) (IntMap.size $ log r)
                           }
                in return (r0, Just msg)
    
    case msg of
        -- New election started, send RequestVoteMsg to all peers
        Just m -> mapM_ (\x -> nsendRemote x "server" m) $ peers c
        -- We were already leader, do nothing
        Nothing -> return ()

    return ()


-- | This thread starts the election timer if it is not killed before then.
delayThread :: ClusterState -> MVar (RaftState a) -> Int -> Process ()
delayThread c mr t = do
    -- Die when parent does
    link $ mainPid c

    -- Delay for t ns
    liftIO $ threadDelay t

    -- Spawn thread that starts election
    spawnLocal $ electionThread c mr

    return ()


-- | Main loop of the program.
raftThread :: ClusterState
           -> MVar (RaftState a)
           -> Process (ClusterState, MVar (RaftState a))
raftThread c mr = do
    -- Die when parent does
    link $ mainPid c

    -- Choose election timeout between 150ms and 300ms
    timeout <- liftIO (uniformR (150000, 300000) (randomGen c) :: IO Int)

    -- Restart election delay thread
    kill (delayPid c) "restart"
    delayPid <- spawnLocal $ delayThread c mr timeout
    let c0 = c { delayPid = delayPid }

    return (c, mr)



initRaft :: Backend -> [LocalNode] -> Process ()
initRaft backend nodes = return ()





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
