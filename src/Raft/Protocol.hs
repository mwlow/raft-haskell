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
type ServerId = Int
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
    | RpcString String                  
    | RpcWhereIsReply WhereIsReply      
    | RpcMonitorRef MonitorRef        
    | RpcProcessMonitorNotification ProcessMonitorNotification
    | RpcAppendEntriesMsg (AppendEntriesMsg a)
    | RpcAppendEntriesResponseMsg AppendEntriesResponseMsg
    | RpcRequestVoteMsg RequestVoteMsg
    | RpcRequestVoteResponseMsg RequestVoteResponseMsg

-- | These wrapping functions simply return the message in an RPCMsg type.
recvSay :: String -> Process (RpcMessage a)
recvSay a = say a >> return RpcNothing

recvEcho :: (ProcessId, String) -> Process (RpcMessage a)
recvEcho (pid, a) = say "received" >> send pid a >> return RpcNothing

recvWhereIsReply :: WhereIsReply -> Process (RpcMessage a)
recvWhereIsReply a =  return $ RpcWhereIsReply a

recvProcessMonitorNotification :: ProcessMonitorNotification 
                               -> Process (RpcMessage a)
recvProcessMonitorNotification a = return $ RpcProcessMonitorNotification a


-- | This is the process's local view of the entire cluster
data ClusterState = ClusterState 
    { backend      :: Backend                    -- ^ Backend for the topology
    , nodeIds      :: [NodeId]                -- ^ List of nodes in the topology
    , processMap   :: Map.Map NodeId ProcessId   -- ^ Map of currently known processIds
    }

-- | This is the node's local state.
data NodeState = NodeState
    { gen         :: GenIO
    , pid         :: ProcessId
    }

-- | This is the main process for handling Raft RPCs.
raftLoop :: ClusterState -> NodeState -> Process ()
raftLoop clusterState@(ClusterState backend nodeIds processMap) 
         nodeState@(NodeState gen pid) = do

    -- Die when parent does    
    --link pid 

    -- Attempt to reconnect to processes that have died
    died <- filterM (fmap not . isProcessAlive . snd) $ Map.toList processMap
    let diedProcesses = map snd died
        diedNodes = map fst died
        deadNodes = diedNodes `union` (nodeIds \\ Map.keys processMap)
    mapM_ reconnect (Map.elems processMap)
    mapM_ reconnect diedProcesses
{-    case (Map.size processMap) of
        0 -> say "0"
        1 -> say "1"
        _ -> return ()
 -}   

    -- Randomly send out probe to update processes
    sefPid <- getSelfPid
    say (show pid)

    --say $ show (length deadNodes)
    rand <- liftIO $ (uniformR (0, length deadNodes - 1) gen :: IO Int)
    unless (null deadNodes) $ do
        --say $ show (deadNodes !! rand)
        whereisRemoteAsync (deadNodes !! rand) "server"

    -- Handle messages. Use timeout to ensure frequent probing.
    msg <- receiveTimeout 250000 [ match recvSay
                                 , match recvEcho
                                 , match recvProcessMonitorNotification
                                 , match recvWhereIsReply
                                 ]

    case msg of
        Nothing -> raftLoop clusterState nodeState
        Just RpcNothing -> raftLoop clusterState nodeState
        Just (RpcWhereIsReply m) -> handleWhereIsReply m 
        _ -> raftLoop clusterState nodeState
  where
    -- If we get WhereIsReply, update our process map
    handleWhereIsReply :: WhereIsReply -> Process ()
    handleWhereIsReply (WhereIsReply _ (Just p)) = do
        let newProcessMap = Map.insert (processNodeId p) p processMap
            newClusterState = ClusterState backend nodeIds newProcessMap
--        say "here"
        raftLoop newClusterState nodeState
    handleWhereIsReply x = do
--        say $ show x
        raftLoop clusterState nodeState


-- | Starts up Raft on the node.
initRaft :: Backend -> [LocalNode] -> Process ()
initRaft backend nodes = do
    -- Hello world!
    say "Yo, I'm alive!"

    -- Create random generator for random events 
    gen <- liftIO createSystemRandom
    self <- getSelfPid
    let nodeIds = localNodeId <$> nodes 
        clusterState = ClusterState backend nodeIds Map.empty
        nodeState = NodeState gen self
    
    -- Start process for handling messages and register it
    serverPid <- spawnLocal (raftLoop clusterState nodeState)
    reg <- whereis "server"
    case reg of
        Nothing -> say "1 here" >> register "server" serverPid
        _ -> say "2 here" >> reregister "server" serverPid

    say $ show (nodeIds)
    mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
   -- mapM_ (\x -> nsendRemote x "server" (serverPid, "ping")) (localNodeId <$> nodes)
--    mapM_ (\x -> nsendRemote x "server" "abcd") (localNodeId <$> nodes)
    x <- receiveWait []
    return ()
    -- start process to handle messages from outside world.
    -- Use forward to forward all messages to the leader.
