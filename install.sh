#!/bin/bash

color=`tput setaf 4`
reset=`tput sgr0`

echo "${color}==> Creating sandbox${reset}"
cabal sandbox init

echo "${color}==> Fetching third party dependencies${reset}"
git submodule init
git submodule update

echo "${color}==> Installing dependencies${reset}"
cabal install sources/cloud-haskell sources/distributed-process-simplelocalnet/
cabal install --only-dependencies

echo "${color}==> Building${reset}"
cabal build
