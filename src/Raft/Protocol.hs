{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveDataTypeable #-}

module Raft.Protocol where

import Data.Binary
import Data.Typeable
import GHC.Generics (Generic)

-- | Create serializable types for RPC.
newtype RpcInt = RpcInt Int deriving (Generic, Typeable)
instance Binary RpcInt

newtype RpcBool = RpcBool Bool deriving (Generic, Typeable)
instance Binary RpcBool

newtype RpcString = RpcString String deriving (Generic, Typeable)
instance Binary RpcString


