#!/bin/bash

git submodule init
git submodule update

cabal sandbox init
cabal install sources/cloud-haskell sources/distributed-process-simplelocalnet/
cabal install --only-dependencies
cabal build
