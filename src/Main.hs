import System.Environment
import System.IO
import System.Console.ANSI
import System.Exit
import System.Posix.Signals
import System.Random.MWC
import System.Directory
import Control.Concurrent 
import Control.Monad 
import Control.Monad.Loops
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Data.Time.Clock
import qualified Data.Map as Map
import qualified Data.IntMap as IntMap

import Data.List
import Data.Function
import Text.Read
import Text.Printf

import Raft.Protocol
import Test

-- | Colors!
setColor :: Color -> IO ()
setColor c = setSGR [SetColor Foreground Dull c]

resetColor :: IO ()
resetColor = setSGR [Reset] >> putStr "" >> hFlush stdout

color :: Color -> IO a -> IO a
color c action = setColor c >> action <* resetColor


commandTests :: Backend -> [LocalNode] -> [ProcessId] -> IO ()
commandTests backend nodes processes = do
    -- Initialize state
    testNode <- newLocalNode backend
    randomGen <- liftIO createSystemRandom
    let z = zip nodes processes
        m = Map.fromList . zip (localNodeId <$> nodes) $ z

    -- Run test loop
    loop m randomGen testNode
  where
    loop :: Map.Map NodeId (LocalNode, ProcessId) -> GenIO -> LocalNode -> IO ()
    loop m randomGen testNode = do
        -- Catch control-c
        tid <- myThreadId
        installHandler keyboardSignal (Catch (cntrlc tid nodes)) Nothing

        -- Randomly select a random number of (node, pid) to stop.
        let r = (uniformR (0, Map.size m - 1) (randomGen) :: IO Int)
        n <- (uniformR (0, Map.size m `div` 2) (randomGen) :: IO Int)
        a <- nub <$> replicateM n r
        let dead = (`Map.elemAt` m) <$> a
            live = Map.difference m (Map.fromList dead)
            r'   = (uniformR (0, Map.size live - 1) (randomGen) :: IO Int)

        --(nodeId, (localnode, processId))
        color Red (print "--")
        mapM_ (\(_, (_, pid)) -> color Yellow (print pid)) dead

        -- Stop them
        forkProcess testNode $ mapM_ (\(n, (_, _)) -> 
            nsendRemote n "state" False) dead 
        --threadDelay 2000000

        -- Send some messages
        t <- (uniformR (1, 5) (randomGen) :: IO Int)
        forkProcess testNode $ forM_ [1..t] $ \i -> do
            r <- liftIO r'
            nsendRemote (fst $ Map.elemAt r live) "client" (Command $ show i)
        threadDelay 2000000
        
        -- Restart them
        --l <- mapM (\(k, (n, _)) -> do
        --        p <- forkProcess n $ initRaft backend nodes
        --        return (k, (n, p))) dead
        color Yellow $ print "Restarting..."
        forkProcess testNode $ mapM_ (\(n, (_, _)) -> 
            nsendRemote n "state" True) dead 
        threadDelay 2000000

        -- Update map
        --let m' = Map.union (Map.fromList l) m

        -- Loop forever
        loop m randomGen testNode

-- | This thread starts a new election.
sendCommandThread :: NodeId -> String -> Process ()
sendCommandThread nodeID command = do
    nsendRemote nodeID "client" (Command $ show command)

commandLoop :: Backend -> [LocalNode] -> [ProcessId] -> IO ()
commandLoop backend nodes processes = do
    -- Initialize state
    randomGen <- liftIO createSystemRandom
    let z = zip [0,1..] (zip3 (localNodeId <$> nodes) nodes processes)
        m = IntMap.fromList z

    tid <- myThreadId
    installHandler keyboardSignal (Catch (cntrlc tid nodes)) Nothing


    input <- getLine
    print input
    let 
        args = words input
        command = args !! 0
        argument = args !! 1
    case command of
        "wake" ->  do
            let 
                nodeID = read argument
                x = IntMap.lookup nodeID m
            case x of
                Just (nodeID, node, process) -> do 
                    print "WAKED NODE"
                    forkProcess node (nsendRemote nodeID "state" True)
                    commandLoop backend nodes processes
                    return ()
                Nothing -> do 
                    print "INVALID NODE SPECIFIED"
                    commandLoop backend nodes processes
                    return ()
        "kill" -> do
            let 
                nodeID = read argument
                x = IntMap.lookup nodeID m
            case x of
                Just (nodeID, node, process) -> do 
                    print "KILLED NODE"
                    forkProcess node (nsendRemote nodeID "state" False)
                    commandLoop backend nodes processes
                    return ()
                Nothing -> do 
                    print "INVALID NODE SPECIFIED"
                    commandLoop backend nodes processes
                    return ()
            
        _ -> do
            let 
                nodeID = read command
                x = IntMap.lookup nodeID m
            case x of
                Just (nodeID, node, process) -> do 
                    print "INPUT RECEIVED"
                    forkProcess node (sendCommandThread nodeID command)
                    commandLoop backend nodes processes
                    return ()
                Nothing -> do 
                    print "INVALID NODE SPECIFIED"
                    commandLoop backend nodes processes
                    return ()
            
        



-- | Handle Control C.
cntrlc :: ThreadId -> [LocalNode] -> IO ()
cntrlc tid nodes = do
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes
    killThread tid

-- | Initialize the cluster.
initCluster :: String -> String -> Int -> IO ()
initCluster host port numNodes = do
    -- Initialize backend and then start all the nodes
    color Cyan $ printf "To kill a node, enter node number\n"
    color Cyan $ printf "Enter node number followed by a space, and then your command\n"
    color Cyan $ printf "%s %d %s\n" "==> Starting up" numNodes "nodes"
    backend <- initializeBackend host port initRemoteTable
    nodes <- replicateM numNodes $ newLocalNode backend
    tid <- myThreadId
    installHandler keyboardSignal (Catch (cntrlc tid nodes)) Nothing

    -- Wait awhile to let nodes start
    threadDelay 500000

    -- Find and count peers
    count <- liftM length $ findPeers backend 500000 >>= mapM print
    putStr "Nodes detected: "
    if count == numNodes
        then color Green $ printf "%d%s%d\n" count "/" numNodes
        else color Red $ printf "%d%s%d\n" count "/" numNodes

    -- Run Raft on all of the nodes
    threadDelay 500000
    color Cyan . putStrLn $ "==> Running Raft ('q' to exit)"
    processes <- mapM (\(n,i) -> forkProcess n $ initRaft backend nodes i) $ zip nodes [0..]
    threadDelay 500000

    -- Run partition experiments...
    --threadDelay 5000000


    --This is the code the spawns the test thread
    -- testNode <- newLocalNode backend
    
    -- runProcess testNode (testCalls nodes processes)

   -- testNode <- newLocalNode backend
    --forkProcess testNode $ initRaft backend nodes 
    --threadDelay 1000000
    --forkProcess (nodes !! 0) $ initRaft backend nodes
    --commandTests backend nodes processes
    

    commandLoop backend nodes processes


    -- Run until receive 'q' or Control C
    whileM_ (liftM ('q' /=) getChar) $
        installHandler keyboardSignal (Catch (cntrlc tid nodes)) Nothing

    -- Clean Up
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes


main :: IO ()
main = do
    -- Set buffering mode for reading in user input
    -- hSetBuffering stdin NoBuffering
    -- hSetBuffering stdout LineBuffering
    -- hSetBuffering stderr LineBuffering

    -- Rename temporary directory if it exists
    exists <- doesDirectoryExist "tmp"
    when exists $ do
        time <- getCurrentTime
        renameDirectory "tmp" $ "tmp-" ++ show time

    -- Recreate it
    createDirectory "tmp"

    -- Parse command line arguments
    args <- getArgs
    case args of
        [host, port, numNodes] ->
            case readMaybe numNodes of
                Nothing -> putStrLn "usage: raft host port numNodes"
                -- Start the cluster
                Just n -> initCluster host port n
        _ -> putStrLn "usage: raft host port numNodes"






