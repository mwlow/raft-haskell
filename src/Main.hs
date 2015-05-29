import System.Environment
import System.IO
import System.Console.ANSI
import System.Random.MWC
import System.Directory
import Control.Concurrent 
import Control.Monad
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Data.Time.Clock
import qualified Data.Map.Strict as Map
import qualified Data.IntMap.Strict as IntMap

import Data.List
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

commandLoop :: Backend -> [LocalNode] -> [ProcessId] -> IO ()
commandLoop backend nodes processes = do
    -- Initialize state
    testNode <- newLocalNode backend
    let m = IntMap.fromList . zip [0..] $ (localNodeId <$> nodes)
    -- Run test loop
    loop m testNode
    closeLocalNode testNode
  where
    loop :: IntMap.IntMap NodeId -> LocalNode -> IO ()
    loop m testNode = do
        args <- words <$> getLine
        case args of
            "kill":index:_ -> do
                let v = readMaybe index >>= \i -> IntMap.lookup i m
                case v of
                    Just nID -> do
                        runProcess testNode $ nsendRemote nID "state" False
                        putStrLn $ "Killed node " ++ show nID
                        loop m testNode
                    Nothing  -> do
                        color Red $ putStrLn "Error in input"
                        loop m testNode
            "wake":index:_ -> do
                let v = readMaybe index >>= \i -> IntMap.lookup i m
                case v of
                    Just nID -> do
                        runProcess testNode $ nsendRemote nID "state" True
                        putStrLn $ "Restarted node " ++ show nID
                        loop m testNode
                    Nothing  -> do 
                        color Red $ putStrLn "Error in input"
                        loop m testNode
            "quit":_ -> return ()
            "send":index:command:_ -> do
                let v = readMaybe index >>= \i -> IntMap.lookup i m
                case v of
                    Just nID -> do
                        runProcess testNode $ 
                            nsendRemote nID "client" (Command $ unwords command)
                        putStrLn $ "Sending message to " ++ show nID
                        loop m testNode
                    Nothing  -> do
                        color Red $ putStrLn "Error in input"
                        loop m testNode
            other@_ -> putStrLn "Error in input" >> loop m testNode


-- | Initialize the cluster.
initCluster :: String -> String -> Int -> IO ()
initCluster host port numNodes = do
    -- Initialize backend and then start all the nodes
    color Cyan $ printf "%s %d %s\n" "==> Starting up" numNodes "nodes"
    backend <- initializeBackend host port initRemoteTable
    nodes <- replicateM numNodes $ newLocalNode backend

    -- Wait awhile to let nodes start
    threadDelay 500000

    -- Find and count peers
    count <- liftM length $ findPeers backend 500000 >>= mapM print
    putStr "Nodes found: "
    if count == numNodes
        then color Green $ printf "%d%s%d\n" count "/" numNodes
        else color Red $ printf "%d%s%d\n" count "/" numNodes

    -- Run Raft on all of the nodes
    threadDelay 500000
    color Cyan . putStrLn $ "==> Running Raft ('quit' to exit)"
    processes <- mapM (\(n,i) -> forkProcess n $ initRaft backend nodes i) $ zip nodes [0..]
    threadDelay 1500000

    color Cyan . putStrLn $ "==> Play with me:"
    putStrLn "kill [nid]         -- Put a node to sleep"
    putStrLn "wake [nid]         -- Wake up the node"
    putStrLn "send [nid] command -- Send a node a message to replicate"
    color Magenta $ putStrLn "quit               -- Exit"
    putStrLn ""
    putStrLn "Example: kill 0"
    putStrLn "         send 1 write me to /tmp"
    putStrLn "         wake 0"
    color Magenta $ putStrLn "         quit"
    putStrLn ""
    putStrLn "I will currently fail if you kill a majority of nodes!"
    putStrLn ""

    -- Run interactive prompt...
    commandLoop backend nodes processes

    -- Clean Up
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes


main :: IO ()
main = do
    -- Set buffering mode for reading in user input
    hSetBuffering stdin  LineBuffering
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering

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






