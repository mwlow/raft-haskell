{-# LANGUAGE DeriveDataTypeable #-}

module Raft.Protocol 
( initRaft
, Command(..)
)
where

import Prelude hiding (log)
import System.Random.MWC
import System.Directory
import System.IO
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad hiding (forM_)
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Internal.Types
import Data.Binary
import Data.Typeable
import Data.List
import Data.Foldable (forM_)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map

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

-- | An entry in a log. Also serializable.
data LogEntry a = LogEntry 
    { termReceived   :: !Term
    , applied        :: !Bool
    , command        :: !(Command a)
    } deriving (Show, Read, Eq, Typeable)

instance (Binary a) => Binary (LogEntry a) where
    put (LogEntry a b c) = put a >> put b >> put c
    get = LogEntry <$> get <*> get <*> get

-- | Data structure for appendEntriesRPC. Is serializable.
data AppendEntriesMsg a = AppendEntriesMsg 
    { aeSender       :: !ProcessId -- ^ Sender's process id
    , aeTerm         :: !Term      -- ^ Leader's term
    , leaderId       :: !NodeId    -- ^ Leader's Id
    , prevLogIndex   :: !Index     -- ^ Log entry index right before new ones
    , prevLogTerm    :: !Term      -- ^ Term of the previous log entry
    , entries        :: !(Log a)   -- ^ Log entries to store, [] for heartbeat
    , leaderCommit   :: !Index     -- ^ Leader's commit index
    } deriving (Show, Eq, Typeable)

instance (Binary a) => Binary (AppendEntriesMsg a) where
    put (AppendEntriesMsg a b c d e f g) =
        put a >> put b >> put c >> put d >> put e >> put f >> put g
    get = AppendEntriesMsg <$> get <*> get <*> get <*> get <*> get <*> get <*> get

-- | Data structure for the response to appendEntriesRPC. Is serializable.
data AppendEntriesResponseMsg = AppendEntriesResponseMsg 
    { aerSender     :: !ProcessId  -- ^ Sender's process id
    , aerTerm       :: !Term       -- ^ Current term to update leader
    , aerMatchIndex :: !Index      -- ^ Index of highest log entry replicated
    , success       :: !Bool       -- ^ Did follower contain a matching entry?
    } deriving (Show, Eq, Typeable)

instance Binary AppendEntriesResponseMsg where
    put (AppendEntriesResponseMsg a b c d) = put a >> put b >> put c >> put d
    get = AppendEntriesResponseMsg <$> get <*> get <*> get <*> get

-- | Data structure for requestVoteRPC. Is serializable.
data RequestVoteMsg = RequestVoteMsg
    { rvSender      :: !ProcessId   -- ^ Sender's process id
    , rvTerm        :: !Term        -- ^ Candidate's term
    , candidateId   :: !NodeId      -- ^ Candidate requesting vote
    , lastLogIndex  :: !Index       -- ^ Index of candidate's last log entry
    , lastLogTerm   :: !Term        -- ^ Term of candidate's last log entry
    } deriving (Show, Eq, Typeable)

instance Binary RequestVoteMsg where
    put (RequestVoteMsg a b c d e) = 
        put a >> put b >> put c >> put d >> put e
    get = RequestVoteMsg <$> get <*> get <*> get <*> get <*> get

-- | Data structure for the response to requestVoteRPC. Is serializable.
data RequestVoteResponseMsg = RequestVoteResponseMsg
    { rvrSender     :: !ProcessId  -- ^ Sender's process id
    , rvrTerm       :: !Term       -- ^ Current term to update leader
    , voteGranted   :: !Bool       -- ^ Did candidate receive vote?
    } deriving (Show, Eq, Typeable)

instance Binary RequestVoteResponseMsg where
    put (RequestVoteResponseMsg a b c) =
        put a >> put b >> put c
    get = RequestVoteResponseMsg <$> get <*> get <*> get

-- | Hack to allow us to process arbitrary messages from the mailbox
data RpcMessage a
    = RpcNothing                
    | RpcWhereIsReply WhereIsReply           
    | RpcProcessMonitorNotification ProcessMonitorNotification
    | RpcAppendEntriesMsg (AppendEntriesMsg a)
    | RpcAppendEntriesResponseMsg AppendEntriesResponseMsg
    | RpcRequestVoteMsg RequestVoteMsg
    | RpcRequestVoteResponseMsg RequestVoteResponseMsg
    | RpcCommandMsg (Command a)
    | RpcStateMsg Bool

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

recvCommandMsg :: Command a -> Process (RpcMessage a)
recvCommandMsg a = return $ RpcCommandMsg a

recvStateMsg :: Bool -> Process (RpcMessage a)
recvStateMsg a = return $ RpcStateMsg a


-- | This is the process's local view of the entire cluster. Information
-- stored in this data structure should only be modified by the main thread.
data ClusterState = ClusterState 
    { backend     :: !Backend       -- ^ Backend for the topology
    , numNodes    :: !Int           -- ^ Number of nodes in the topology
    , selfNodeId  :: !NodeId        -- ^ nodeId of current server
    , nodeIds     :: ![NodeId]      -- ^ List of nodeIds in the topology
    , peers       :: ![NodeId]      -- ^ nodeIds \\ [selfNodeId]
    , mainPid     :: !ProcessId     -- ^ ProcessId of the master thread
    , raftPid     :: !ProcessId     -- ^ ProcessId of the thread running Raft
    , clientPid   :: !ProcessId     -- ^ ProcessId of the state thread
    , delayPid    :: !ProcessId     -- ^ ProcessId of the delay thread
    , randomGen   :: GenIO          -- ^ Random generator for random events
    , identifier  :: !Int           -- ^ Identifier
    }

-- | Creates a new cluster.
newCluster :: Backend -> [LocalNode] -> Int -> Process ClusterState
newCluster backend nodes identifier = do 
    selfPid   <- getSelfPid
    randomGen <- liftIO createSystemRandom
    return ClusterState {
        backend    = backend
      , numNodes   = length nodes
      , selfNodeId = processNodeId selfPid
      , nodeIds    = localNodeId <$> nodes 
      , peers      = (localNodeId <$> nodes) \\ [processNodeId selfPid]
      , mainPid    = selfPid
      , raftPid    = nullProcessId (processNodeId selfPid)
      , clientPid  = nullProcessId (processNodeId selfPid)
      , delayPid   = nullProcessId (processNodeId selfPid)
      , randomGen  = randomGen
      , identifier = identifier
    }

-- | This is state that is volatile and can be modified by multiple threads
-- concurrently. This state will always be passed to the threads in an MVar.
data RaftState a = RaftState
    { leader          :: !(Maybe NodeId)       -- ^ Current leader of Raft   
    , state           :: !ServerRole           -- ^ Follower, Candidate, Leader
    , currentTerm     :: !Term                 -- ^ Current election term
    , votedFor        :: !(Maybe NodeId)       -- ^ Voted node for leader
    , log             :: !(Log a)              -- ^ Contains log entries
    , commitIndex     :: !Index                -- ^ Highest entry committed
    , lastApplied     :: !Index                -- ^ Highest entry applied

    , nextIndexMap    :: Map.Map NodeId Index  -- ^ Next entry to send
    , matchIndexMap   :: Map.Map NodeId Index  -- ^ Highest entry replicated

    , voteCount       :: !Int                  -- ^ Number votes received
    , active          :: !Bool                 -- ^ Is Raft active on this node?
    }

-- | Creates a new RaftState.
newRaftState :: [LocalNode] -> RaftState a
newRaftState nodes = RaftState {
        leader        = Nothing
      , state         = Follower
      , currentTerm   = 0
      , votedFor      = Nothing
      , log           = IntMap.empty 
      , commitIndex   = 0
      , lastApplied   = 0
      , nextIndexMap  = Map.fromList $ zip (localNodeId <$> nodes) [1, 1..]
      , matchIndexMap = Map.fromList $ zip (localNodeId <$> nodes) [0, 0..]
      , voteCount     = 0
      , active        = True
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
                     -> MVar (RaftState a) 
                     -> RequestVoteMsg
                     -> Process ()
handleRequestVoteMsg c mr msg = do
    (t, g) <- liftIO $ modifyMVarMasked mr $ \r -> handleMsg c r msg
    send (rvSender msg) (RequestVoteResponseMsg (raftPid c) t g)
  where
    handleMsg :: ClusterState
              -> RaftState a 
              -> RequestVoteMsg 
              -> IO (RaftState a, (Term, Bool))
    handleMsg c r
        msg@(RequestVoteMsg sender term candidateId lastLogIndex lastLogTerm)
        | term > rCurrentTerm = handleMsg c (stepDown r term) msg
        | term < rCurrentTerm = return (r, (rCurrentTerm, False))
        | rVotedFor `elem` [Nothing, Just candidateId]
        , lastLogTerm > rLastLogTerm || 
              lastLogTerm == rLastLogTerm && lastLogIndex >= rLogSize
        = return (r { votedFor = Just candidateId }, (term, True))
        | otherwise = return (r, (rCurrentTerm, False))
      where
        rLog           = log r
        rLogSize       = IntMap.size rLog
        rVotedFor      = votedFor r
        rCurrentTerm   = currentTerm r
        rLastLogTerm   = logTerm (log r) rLogSize


-- | Handle Request Vote response from peer
handleRequestVoteResponseMsg :: ClusterState
                             -> MVar (RaftState a)
                             -> RequestVoteResponseMsg
                             -> Process ()
handleRequestVoteResponseMsg c mr msg =
    liftIO $ modifyMVarMasked_ mr $ \r -> handleMsg c r msg
  where
    handleMsg :: ClusterState 
              -> RaftState a 
              -> RequestVoteResponseMsg 
              -> IO (RaftState a)
    handleMsg c r msg@(RequestVoteResponseMsg sender term granted)
        | term > rCurrentTerm = handleMsg c (stepDown r term) msg
        | rState == Candidate
        , rCurrentTerm == term
        , granted 
        = return r { voteCount = rVoteCount + 1 }
        | otherwise = return r
      where
        rState         = state r
        rCurrentTerm   = currentTerm r
        rVoteCount     = voteCount r


-- | Handle Append Entries request from peer
handleAppendEntriesMsg :: ClusterState
                       -> MVar (RaftState a)
                       -> AppendEntriesMsg a
                       -> Process ()
handleAppendEntriesMsg c mr msg = do
    (t, g, i) <- liftIO $ modifyMVarMasked mr $ \r -> handleMsg c r msg
    --say "Received append entries msg"
    --say $ show t ++ " " ++ show i ++" "++ show g
    send (aeSender msg) (AppendEntriesResponseMsg (raftPid c) t i g)
  where
    handleMsg :: ClusterState
              -> RaftState a 
              -> AppendEntriesMsg a
              -> IO (RaftState a, (Term, Bool, Index))
    handleMsg c r 
        msg@(AppendEntriesMsg
            sender term leaderId prevLogIndex prevLogTerm entries leaderCommit)
        | term > rCurrentTerm = handleMsg c (stepDown r term) msg
        | term < rCurrentTerm = return (r, (rCurrentTerm, False, 0))
        | prevLogIndex == 0 || 
            (prevLogIndex <= rLogSize && 
                logTerm (log r) prevLogIndex == prevLogTerm) = do
            let rLog' = appendLog rLog entries prevLogIndex
                index = prevLogIndex + IntMap.size entries
                r'    = r { 
                            log         = rLog'
                          , commitIndex = min leaderCommit index
                          , leader      = Just $ processNodeId sender
                          , state       = Follower
                          }
            return (r', (rCurrentTerm, True, index))
        | otherwise = return (r { 
                        leader = Just $ processNodeId sender
                      , state  = Follower
                      } , (rCurrentTerm, False, 0))
      where
        rLog           = log r
        rLogSize       = IntMap.size rLog
        rVotedFor      = votedFor r
        rCurrentTerm   = currentTerm r
        rLastLogTerm   = logTerm (log r) rLogSize
        appendLog :: Log a -> Log a -> Index -> Log a
        appendLog dstLog srcLog prevLogIndex
            | IntMap.null srcLog = dstLog
            | logTerm dstLog index /= termReceived headSrcLog = 
                appendLog (IntMap.insert index headSrcLog initDstLog) tailSrcLog index
            | otherwise = appendLog dstLog tailSrcLog index
          where
            index = prevLogIndex + 1
            ((_, headSrcLog), tailSrcLog) = IntMap.deleteFindMin srcLog
            initDstLog
                | prevLogIndex < IntMap.size dstLog = 
                    IntMap.filterWithKey (\k _ -> k <= prevLogIndex) dstLog
                | otherwise = dstLog


-- | Handle Append Entries response from peer.
handleAppendEntriesResponseMsg :: ClusterState
                               -> MVar (RaftState a)
                               -> AppendEntriesResponseMsg
                               -> Process ()
handleAppendEntriesResponseMsg c mr msg =
    liftIO $ modifyMVarMasked_ mr $ \r -> handleMsg c r msg
  where
    handleMsg :: ClusterState
              -> RaftState a 
              -> AppendEntriesResponseMsg
              -> IO (RaftState a)
    handleMsg c r msg@(AppendEntriesResponseMsg sender term matchIndex success)
        | term > rCurrentTerm = handleMsg c (stepDown r term) msg
        | rState == Leader
        , rCurrentTerm == term 
        = if success
              then return r { 
                  matchIndexMap = Map.insert peer matchIndex rMatchIndexMap
                , nextIndexMap = Map.insert peer (matchIndex + 1) rNextIndexMap
                } 
              else return r {
                  nextIndexMap = Map.insert peer (max 1 $ rNextIndex - 1) rNextIndexMap
                }
        | otherwise = return r
      where
        peer           = processNodeId sender
        rState         = state r
        rCurrentTerm   = currentTerm r
        rMatchIndexMap = matchIndexMap r
        rNextIndexMap  = nextIndexMap r
        rNextIndex     = rNextIndexMap Map.! peer


-- | Handle command message from a client. If leader, write to log. Else
-- forward to leader. If there is no leader...send back to self
handleCommandMsg :: Serializable a
                 => ClusterState
                 -> MVar (RaftState a)
                 -> Command a
                 -> Process ()
handleCommandMsg c mr msg = do
    (forwardMsg, leader) <- liftIO $ modifyMVarMasked mr $ \r -> handleMsg c r msg
    case leader of
        Nothing -> when forwardMsg $ void (nsend "client" msg)
        Just l  -> when forwardMsg $ void (nsendRemote l "client" msg)
  where
    handleMsg :: ClusterState 
              -> RaftState a 
              -> Command a 
              -> IO (RaftState a, (Bool, Maybe NodeId))
    handleMsg c r msg
        | state r /= Leader = return (r, (True, leader r))
        | otherwise = do
            let key = 1 + IntMap.size (log r)
                entry = LogEntry {
                  termReceived = currentTerm r
                , applied      = False
                , command      = msg
                }
            return ( r { log = IntMap.insert key entry (log r) }, (False, leader r))


-- | Applies entries in the log to state machine.
applyLog :: Show a => ClusterState -> MVar (RaftState a) -> Process ()
applyLog c mr = do
    finished <- liftIO $ modifyMVarMasked mr $ \r ->
        if lastApplied r >= commitIndex r 
            then return (r, True)
            else 
                let lastApplied' = succ . lastApplied $ r
                    entry        = log r IntMap.! lastApplied'
                    entry'       = entry { applied = True }
                    fname        = "tmp/" ++ show (identifier c) ++ ".csv"
                    Command cm   = command entry
                    line         = show (termReceived entry) ++ "," ++ show cm
                in do
                    withFile fname AppendMode $ \h -> do
                        hPutStrLn h line >> hFlush h
                    return (r {
                      lastApplied = lastApplied'
                    ,   log         = IntMap.insert lastApplied' entry' (log r)
                    }, False)

    -- Loop until finish applying all
    --l <- liftIO $ withMVarMasked mr $ \r -> return $ log r
    --say $ show (IntMap.size l)

    unless finished $ applyLog c mr


-- | This thread starts a new election.
electionThread :: ClusterState -> MVar (RaftState a) -> Process ()
electionThread c mr = do
    -- Die when parent does
    link $ mainPid c

    -- Update raftState and create (Maybe RequestVoteMsg)
    msg <- liftIO $ modifyMVarMasked mr $ \r ->
        if state r == Leader
            then return (r, Nothing)
            else 
                let r' = r { state          = Candidate 
                           , votedFor       = Just $ selfNodeId c
                           , currentTerm    = 1 + currentTerm r
                           , voteCount      = 1
                           , matchIndexMap  = Map.fromList $ zip (nodeIds c) [0, 0..]
                           , nextIndexMap   = Map.fromList $ zip (nodeIds c) [1, 1..]
                           }
                    msg = RequestVoteMsg 
                           { rvSender     = raftPid c 
                           , rvTerm       = currentTerm r'
                           , candidateId  = selfNodeId c
                           , lastLogIndex = IntMap.size $ log r
                           , lastLogTerm  = logTerm (log r) (IntMap.size $ log r)
                           }
                in return (r', Just msg)
    
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
    active <- liftIO $ withMVarMasked mr $ \r-> return $ active r
    when active $ spawnLocal (electionThread c mr) >> return ()


-- | This thread only performs work when the server is the leader.
leaderThread :: (Show a, Serializable a) 
             => ClusterState -> MVar (RaftState a) -> Int -> Process ()
leaderThread c mr u = do
    -- Die when parent does
    link $ mainPid c

    active <- liftIO $ withMVarMasked mr $ \r-> return $ active r
    unless active (liftIO (threadDelay 10000) >> leaderThread c mr u)

    -- Delay u ns to main update rate
    liftIO $ threadDelay u

    -- Create AppendEntries
    msgsM <- liftIO $ modifyMVarMasked mr $ \r -> handleLeader c r

    -- Send them if they were created
    forM_ msgsM (mapM_ (\ (n, m) -> nsendRemote n "server" m))


    -- Advance commit index
    liftIO $ modifyMVarMasked_ mr $ \r -> do
        let n = filter (\v -> logTerm (log r) v == currentTerm r)
                        [commitIndex r + 1 .. IntMap.size $ log r]
            pred :: Index -> Bool
            pred v = numNodes c `div` 2 <
                        Map.size (Map.filter (>=v) $ matchIndexMap r) + 1
            ns = filter pred n

        if state r == Leader && not (null ns)
            then return r { commitIndex = head ns }
            else return r

    -- Loop forever
    leaderThread c mr u

  where
    handleLeader :: ClusterState 
                 -> RaftState a 
                 -> IO (RaftState a, Maybe [(NodeId, AppendEntriesMsg a)])
    handleLeader c r
        | state r == Leader = return $ foldr (\n (r', Just a) ->
            let r'MatchIndexMap = matchIndexMap r'
                r'NextIndexMap  = nextIndexMap r'
                r'Log           = log r'
                r'LogSize       = IntMap.size r'Log
                r'NextIndex     = r'NextIndexMap Map.! n
                r'LastIndex     = r'LogSize
                r'MatchIndex    = r'MatchIndexMap Map.! n
            in 
                let msg = AppendEntriesMsg {
                        aeSender     = raftPid c
                      , aeTerm       = currentTerm r'
                      , leaderId     = selfNodeId c
                      , prevLogIndex = r'NextIndex - 1
                      , prevLogTerm  = logTerm r'Log $ r'NextIndex - 1
                      , entries      = IntMap.filterWithKey (\k _ -> k >= r'NextIndex) r'Log 
                      , leaderCommit = commitIndex r'
                      }
                    r'' = r' { 
                        nextIndexMap = Map.insert n r'LastIndex r'NextIndexMap
                      }
                in (r', Just $ (n, msg):a)
            ) (r, Just []) (peers c)
        | otherwise = return (r, Nothing)


-- | This thread handles commands from clients and forwards them
-- to the leader.
clientThread :: Serializable a => ClusterState -> MVar (RaftState a) -> Process ()
clientThread c mr = do 
    -- Die when parent does
    link $ mainPid c
   
    (RpcCommandMsg m) <- receiveWait [ match recvCommandMsg ]

    handleCommandMsg c mr m

    -- loop forever
    clientThread c mr

-- | This thread listens from the main program and halts and continues
-- the node (in order to test Raft).
stateThread :: ClusterState -> MVar (RaftState a) -> Process ()
stateThread c mr = do
    msg <- receiveWait [ match recvStateMsg ]

    case msg of 
        RpcStateMsg True -> do
            -- Enable RaftThread
            liftIO $ modifyMVarMasked_ mr $ \r -> return r { active = True }

            -- Get registry values of server and client
            raftReg   <- whereis "server"
            clientReg <- whereis "client"

            -- register them
            case raftReg of
                Nothing -> register "server" $ raftPid c
                _       -> reregister "server" $ raftPid c
            case clientReg of
                Nothing -> register "client" $ clientPid c
                _       -> reregister "client" $ clientPid c

        RpcStateMsg False -> do
            -- Disable RaftThread
            liftIO $ modifyMVarMasked_ mr $ \r -> return r { active = False }

            -- Get registry values of server and client
            raftReg   <- whereis "server"
            clientReg <- whereis "client"

            -- Unregister them
            case raftReg of
                Nothing -> return ()
                _       -> unregister "server"
            case clientReg of
                Nothing -> return ()
                _       -> unregister "client" 

    -- Loop forever
    stateThread c mr


-- | Main loop of the program.
raftThread :: (Show a, Serializable a) => ClusterState -> MVar (RaftState a) -> Process ()
raftThread c mr = do
    -- Die when parent does
    link $ mainPid c

    active <- liftIO $ withMVarMasked mr $ \r-> return $ active r
    unless active (liftIO (threadDelay 10000) >> raftThread c mr)
   
   -- Choose election timeout between 150ms and 600ms
    timeout <- liftIO (uniformR (150000, 300000) (randomGen c) :: IO Int)

    -- Update cluster with raftPid
    selfPid <- getSelfPid
    let c' = c { raftPid = selfPid }

    -- Restart election delay thread
    kill (delayPid c') "restart"
    delayPid <- spawnLocal $ delayThread c' mr timeout
    let c'' = c' { delayPid = delayPid }

    -- Block for RPC Messages
    msg <- receiveWait
      [ match recvAppendEntriesMsg
      , match recvAppendEntriesResponseMsg
      , match recvRequestVoteMsg
      , match recvRequestVoteResponseMsg ]

    -- Handle RPC Message
    case msg of
        RpcAppendEntriesMsg m -> handleAppendEntriesMsg c'' mr m
        RpcAppendEntriesResponseMsg m -> handleAppendEntriesResponseMsg c'' mr m
        RpcRequestVoteMsg m -> handleRequestVoteMsg c'' mr m
        RpcRequestVoteResponseMsg m -> handleRequestVoteResponseMsg c'' mr m
        other@_ -> return ()

    -- Update if become leader
    s <- liftIO $ modifyMVarMasked mr $ \r ->
        case state r of
            Candidate -> if voteCount r > numNodes c'' `div` 2
                then return (r {
                    state        = Leader
                  , leader       = Just $ selfNodeId c''
                  , nextIndexMap = Map.fromList $
                        zip (nodeIds c'') $ repeat $ 1 + IntMap.size (log r)
                }, True)
                else return (r, False)
            other@_   -> return (r, False)

    t <- liftIO $ withMVarMasked mr $ \r -> return $ currentTerm r
    when s $ say $ "I am leader of term: " ++ show t
    
    -- Advance state machine
    applyLog c'' mr

    -- Loop Forever
    raftThread c'' mr



initRaft :: Backend -> [LocalNode] -> Int -> Process ()
initRaft backend nodes identifier = do
    say "Hi!"
    -- Initialize state
    c <- newCluster backend nodes identifier
    mr <- liftIO . newMVar $ (newRaftState nodes :: RaftState String)

    let fname = "tmp/" ++ show identifier ++ ".csv"
        line  = "term,command"
    liftIO $ withFile fname AppendMode $ \h -> do 
        hPutStrLn h line >> hFlush h

    -- Start process for handling messages and register it
    raftPid <- spawnLocal $ raftThread c mr
    raftReg <- whereis "server"
    case raftReg of
        Nothing -> register "server" raftPid
        _       -> reregister "server" raftPid

    -- Update raftPid
    let c' = c { raftPid = raftPid }

    -- Start leader thread
    leaderPid <- spawnLocal $ leaderThread c' mr 50000

    -- Start client thread and register it
    clientPid <- spawnLocal $ clientThread c' mr
    clientReg <- whereis "client"
    case clientReg of
        Nothing -> register "client" clientPid
        _       -> reregister "client" clientPid

    let c'' = c' { clientPid = clientPid }

    -- Start state thread and register it
    statePid <- spawnLocal $ stateThread c'' mr
    stateReg <- whereis "state"
    case stateReg of
        Nothing -> register "state" statePid
        _       -> reregister "state" statePid

    -- Kill this process if raftThread dies
    link raftPid
    link leaderPid
    link clientPid
    link statePid

    -- Hack to block
    x <- receiveWait []

    return ()
