# raft-haskell

This project demonstrates the Raft algorithm, implemented using Haskell.
Requirements include:

* GHC 7.8.4 (broken on 7.10 because of dependencies)
* cabal-install

Build the program via `sh install.sh`, which will first install all
dependencies into a sandbox located in the root directory of the
repository. To run, either:

* `cabal run test [numNodes]` to run a randomized test case.
* `cabal run localhost [port] [numNodes]` to interact manually with Raft.

The number of nodes should be greater than 2.

CSV files, one per node, will be written to the tmp directory. Each
file contains the entries of the node's log that have been applied and thus represents
the current state of the node's state machine.

To verify correctness, after each run, compare the CSV files in the tmp directory to ensure that they are all the same. The automated test (i.e. `cabal run test [numNodes]`) will automatically call `diff` on all the CSV files.
