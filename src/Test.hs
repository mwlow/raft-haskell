module Test where

import System.Random.MWC
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Control.Applicative
import Control.Distributed.Process
import Control.Distributed.Process.Node hiding (newLocalNode)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Internal.Types
import qualified Data.Map as Map

import Raft.Protocol


testCalls :: [LocalNode] -> [a0] -> Process()
testCalls nodes processes = do  
  -- Code to run tests. Zip node and processes into a map
  -- then send to test calls
  let
    nodeIDs = map localNodeId nodes
    nodeList = zip nodeIDs processes
    m = Map.fromList (nodeList)
  randomGen <- liftIO createSystemRandom

  randomValue <- liftIO (uniformR (0, Map.size m - 1) (randomGen) :: IO Int)
  let
    randomNodeId = Map.keys m !! randomValue
    randomProcessId = Map.lookup randomNodeId m

  liftIO (threadDelay 4000000)
  nsendRemote randomNodeId "client" (Command "GAVIN")
  testCalls nodes processes
  return()