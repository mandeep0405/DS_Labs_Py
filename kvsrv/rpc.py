#!/usr/bin/env python3
"""
KV Server RPC Types and Constants
Matching MIT 6.5840 Lab 2 kvsrv1/rpc package
"""

from dataclasses import dataclass
from typing import Union

# Error types
class Err:
    """Error constants matching Go rpc package"""
    OK = "OK"
    ErrNoKey = "ErrNoKey"
    ErrVersion = "ErrVersion"
    ErrMaybe = "ErrMaybe"
    ErrWrongLeader = "ErrWrongLeader"
    ErrWrongGroup = "ErrWrongGroup"

# Type aliases
Tversion = int  # Version type (uint64 in Go)

@dataclass
class PutArgs:
    """Put RPC arguments"""
    Key: str
    Value: str
    Version: Tversion

@dataclass
class PutReply:
    """Put RPC reply"""
    Err: str

@dataclass
class GetArgs:
    """Get RPC arguments"""
    Key: str

@dataclass
class GetReply:
    """Get RPC reply"""
    Value: str
    Version: Tversion
    Err: str