import System.Environment
import System.IO
import System.Console.ANSI
import System.Random.MWC
import System.Directory
import qualified System.Process as SP
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
import Safe

import Raft.Protocol
import Test

-- | Colors!
setColor :: Color -> IO ()
setColor c = setSGR [SetColor Foreground Dull c]

resetColor :: IO ()
resetColor = setSGR [Reset] >> putStr "" >> hFlush stdout

color :: Color -> IO a -> IO a
color c action = setColor c >> action <* resetColor

-- | Test
commandTest :: Backend -> [LocalNode] -> IO ()
commandTest backend nodes = do
    testNode <- newLocalNode backend
    randomGen <- liftIO createSystemRandom
    let l = length nodes
        r = uniformR (0, l - 1) randomGen :: IO Int
        nodeIds = localNodeId <$> nodes

    forM_ [1..10] $ \z -> do
        color Cyan $ printf "%s %d/10\n" "==> Test" (z::Int)

        -- Select a random number of nodes to kill
        n <- uniformR (1, l `div` 2) randomGen :: IO Int
        i <- nub <$> replicateM n r
        let dead = map (\i -> nodeIds !! i) i
            live = nodeIds \\ dead

        forM_ dead $ \nID -> do
            putStrLn $ "kill " ++ show nID
            runProcess testNode $ nsendRemote nID "state" False
        threadDelay 2000000

        -- Send 1-10 messages to each of the remaining nodes
        putStrLn "Sending messages..."
        let m = uniformR (0, 999999) randomGen :: IO Int
        forM_ live $ \nID -> do
            n <- uniformR (1, 10) randomGen :: IO Int
            runProcess testNode $ replicateM_ n $ do
                msg <- liftIO m
                nsendRemote nID "client" (Command $ show msg)
        threadDelay 2000000

        -- Restart nodes
        forM_ dead $ \nID -> do 
            putStrLn $ "wake " ++ show nID
            runProcess testNode $ nsendRemote nID "state" True
        threadDelay 5000000

    closeLocalNode testNode

    color Cyan $ putStrLn "==> Running diff on generated log files"
    threadDelay 2000000
    let pairs = do
            f <- [0..l-1]
            s <- [f..l-1]
            guard (s /= f)
            return (show f ++ ".csv", show s ++ ".csv")

    forM_ pairs $ \p -> do
        uncurry (printf "diff tmp/%s tmp/%s\n") p
        SP.createProcess (SP.proc "diff" [fst p, snd p]) {SP.cwd = Just "tmp"}


-- | Loop that interacts from the user and allows the user to
-- send commands to the cluster.
commandLoop :: Backend -> [LocalNode] -> IO ()
commandLoop backend nodes = do
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
            "send":index:c -> do
                let v = headMay c >> readMaybe index >>= \i -> IntMap.lookup i m
                case v of
                    Just nID -> do
                        runProcess testNode $ 
                            nsendRemote nID "client" (Command $ unwords c)
                        putStrLn $ "Sending message to " ++ show nID
                        loop m testNode
                    Nothing  -> do
                        color Red $ putStrLn "Error in input"
                        loop m testNode
            other@_ -> putStrLn "Error in input" >> loop m testNode


-- | Initialize the cluster.
initCluster :: String -> String -> Int -> Bool -> IO ()
initCluster host port numNodes test = do
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
    if test
        then color Cyan . putStrLn $ "==> Running Raft (test mode)"
        else color Cyan . putStrLn $ "==> Running Raft ('quit' to exit)"
    processes <- mapM (\(n,i) -> forkProcess n $ initRaft backend nodes i) $ zip nodes [0..]
    threadDelay 1500000

    if test
        then do
            color Cyan . putStrLn $ "==> Starting tests"
            commandTest backend nodes
        else do
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
            commandLoop backend nodes

    -- Clean Up
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes


main :: IO ()
main = do
    --createProcess (proc "diff" ["0.csv", "3.csv"]) {cwd = Just "tmp"}
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
                Just n -> initCluster host port n False
        other -> if "test" `elem` other
            then do
                randomGen <- liftIO createSystemRandom
                port <- uniformR (3000, 6000) randomGen :: IO Int
                initCluster "localhost" (show port) 11 True
            else putStrLn "usage: raft host port numNodes"






