
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
import Text.Read
import Text.Printf

import Raft.Protocol

-- | Colors!
setColor :: Color -> IO ()
setColor c = setSGR [SetColor Foreground Dull c]

resetColor :: IO ()
resetColor = setSGR [Reset]

color :: Color -> IO a -> IO a
color c action = setColor c >> action <* resetColor

-- | Cleanup on exit.
cleanUp :: [LocalNode] -> IO ()
cleanUp nodes = do
    color Cyan $ putStrLn "==> Cleaning up"
    mapM_ closeLocalNode nodes

-- | Initialize the cluster.
initCluster :: String -> String -> Int -> IO ()
initCluster host port numServers = do
    -- Initialize backend and then start all the nodes
    color Cyan $ printf "%s %d %s\n" "==> Starting up" numServers "nodes"
    backend <- initializeBackend host port initRemoteTable
    nodes <- replicateM numServers $ newLocalNode backend
    installHandler keyboardSignal (Catch (cleanUp nodes)) Nothing

    -- Wait awhile to let nodes start
    threadDelay 1000000

    -- Find and count peers
    count <- liftM length $ findPeers backend 1000000 >>= mapM print
    putStr "Nodes detected: "
    if count == numServers
        then color Green $ printf "%d%s%d\n" count "/" numServers
        else color Red $ printf "%d%s%d\n" count "/" numServers

    -- Run Raft on all of the nodes
    threadDelay 500000
    color Cyan . putStrLn $ "==> Running Raft"
    processes <- mapM (`forkProcess` initRaft backend nodes) nodes
    
    -- Run partition experiments...
    threadDelay 500000
    
    -- Clean up
    cleanUp nodes


main :: IO ()
main = do
    -- Parse command line arguments
    args <- getArgs
    case args of
        [host, port, numServers] ->
            case readMaybe numServers of
                Nothing -> putStrLn "usage: raft host port numServers"
                -- Start the cluster
                Just n -> initCluster host port n
        _ -> putStrLn "usage: raft host port numServers"

  {-
  backend <- initializeBackend "127.0.0.1" "10501" initRemoteTable
  node <- newLocalNode backend
  node2 <- newLocalNode backend
  liftIO $ threadDelay (1*1000000)
  findPeers backend 1000000 >>= print

  forkProcess node $ do
    echoPid <- spawnLocal $ forever $ do
      receiveWait [match logMessage, match replyBack]


    say "send some messages!"
    send echoPid "hello"
    self <- getSelfPid
    send echoPid (self, [True::Bool])

    m <- expectTimeout 1000000
    case m of
      Nothing  -> die "nothing came back!"
      (Just s) -> say $ "got " ++ s ++ " back!"
    return ()

  liftIO $ threadDelay (1*1000000)
  return ()
  -}




