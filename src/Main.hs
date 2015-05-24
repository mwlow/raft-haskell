import System.Environment
import System.IO
import System.Console.ANSI
import System.Exit
import System.Posix.Signals
import System.Random.MWC
import Control.Concurrent 
import Control.Monad 
import Control.Monad.Loops
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Text.Read
import Text.Printf

import Raft.Protocol

-- | Colors!
setColor :: Color -> IO ()
setColor c = setSGR [SetColor Foreground Dull c]

resetColor :: IO ()
resetColor = setSGR [Reset] >> putStr "" >> hFlush stdout

color :: Color -> IO a -> IO a
color c action = setColor c >> action <* resetColor


commandTests :: [LocalNode] -> Process ()
commandTests nodes = do
    randomGen <- liftIO createSystemRandom
    index <- liftIO (uniformR (0, length nodes - 1) (randomGen) :: IO Int)
    
    let id = localNodeId $ nodes !! index
    say $ show id
    nsendRemote id "client" (Command "test")


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
    processes <- mapM (`forkProcess` initRaft backend nodes) nodes
    
    -- Create a client node to run tests
    clientNode <- newLocalNode backend

    -- Run command test on client
    threadDelay 5000000
    runProcess clientNode (commandTests nodes)


    -- Run partition experiments...
    --threadDelay 5000000

    --runProcess (nodes !! 0) (exit (processes !! 0) "ehh") 

   -- testNode <- newLocalNode backend
    --forkProcess testNode $ initRaft backend nodes 
    --threadDelay 1000000
    --forkProcess (nodes !! 0) $ initRaft backend nodes
    
    -- Run until receive 'q' or Control C
    whileM_ (liftM ('q' /=) getChar) $
        installHandler keyboardSignal (Catch (cntrlc tid nodes)) Nothing

    --closeLocalNode testNode
    -- Clean Up
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes


main :: IO ()
main = do
    -- Set buffering mode for reading in user input
    hSetBuffering stdin NoBuffering
    hSetBuffering stdout LineBuffering
    hSetBuffering stderr LineBuffering

    -- Parse command line arguments
    args <- getArgs
    case args of
        [host, port, numNodes] ->
            case readMaybe numNodes of
                Nothing -> putStrLn "usage: raft host port numNodes"
                -- Start the cluster
                Just n -> initCluster host port n
        _ -> putStrLn "usage: raft host port numNodes"






