#!/usr/bin/env python3
"""
KV Server Implementation
Matching MIT 6.5840 Lab 2 kvsrv1/server.go

This implements a linearizable key/value server with versioned operations.
Each key has an associated version number that tracks how many times it has been written.
"""

import threading
import logging
from typing import Dict, Tuple, Optional, List
from dataclasses import dataclass

# Import our RPC types and labrpc
from rpc import Err, GetArgs, GetReply, PutArgs, PutReply, Tversion
from labrpc import ClientEnd

logger = logging.getLogger(__name__)

# Debug flag - set to True to enable debug logging
DEBUG = False


@dataclass
class KeyValue:
    """Internal representation of a key-value pair with version"""
    value: str
    version: Tversion

class KVServer:
    """
    Key/Value Server Implementation
    
    Maintains an in-memory map of keys to (value, version) pairs.
    Provides linearizable Put and Get operations with version control.
    """
    
    def __init__(self):
        """Initialize KV server"""
        self.mu = threading.Lock()
        # Map from key to KeyValue (value, version)
        self.data: Dict[str, KeyValue] = {}
        
    def Get(self, args: GetArgs, reply: GetReply):
        """
        Get RPC handler
        
        Returns the current value and version for a key.
        Returns ErrNoKey if the key doesn't exist.
        """
        with self.mu:
            logger.debug("KVServer.Get: key=%s", args.Key)
            
            if args.Key in self.data:
                kv = self.data[args.Key]
                reply.Value = kv.value
                reply.Version = kv.version
                reply.Err = Err.OK
                logger.debug("KVServer.Get: key=%s, value=%s, version=%d -> OK", 
                       args.Key, reply.Value, reply.Version)
            else:
                reply.Value = ""
                reply.Version = 0
                reply.Err = Err.ErrNoKey
                logger.debug("KVServer.Get: key=%s -> ErrNoKey", args.Key)
                
    def Put(self, args: PutArgs, reply: PutReply):
        """
        Put RPC handler
        
        Updates the value for a key if the version matches the server's version.
        - If versions match: update value, increment version, return OK
        - If versions don't match: return ErrVersion
        - If key doesn't exist and version=0: create new key with version=1
        - If key doesn't exist and version>0: return ErrNoKey
        """
        with self.mu:
            logger.debug("KVServer.Put: key=%s, value=%s, version=%d", 
                   args.Key, args.Value, args.Version)
            
            if args.Key in self.data:
                # Key exists - check version
                current_kv = self.data[args.Key]
                if current_kv.version == args.Version:
                    # Version matches - update value and increment version
                    new_version = current_kv.version + 1
                    self.data[args.Key] = KeyValue(args.Value, new_version)
                    reply.Err = Err.OK
                    logger.debug("KVServer.Put: key=%s updated, version %d -> %d", 
                           args.Key, args.Version, new_version)
                else:
                    # Version mismatch
                    reply.Err = Err.ErrVersion
                    logger.debug("KVServer.Put: key=%s version mismatch, expected=%d, got=%d -> ErrVersion", 
                           args.Key, current_kv.version, args.Version)
            else:
                # Key doesn't exist
                if args.Version == 0:
                    # Create new key with version 1
                    self.data[args.Key] = KeyValue(args.Value, 1)
                    reply.Err = Err.OK
                    logger.debug("KVServer.Put: key=%s created with version=1", args.Key)
                else:
                    # Key doesn't exist but version > 0
                    reply.Err = Err.ErrNoKey
                    logger.debug("KVServer.Put: key=%s doesn't exist, version=%d -> ErrNoKey", 
                           args.Key, args.Version)
                    
    def Kill(self):
        """Kill the server (for testing)"""
        pass


def MakeKVServer() -> KVServer:
    """Create a new KV server instance"""
    return KVServer()


def StartKVServer(ends: List[ClientEnd], gid: int, srv: int, persister) -> List:
    """
    Start KV server - matching Go interface
    
    This function is called by the test infrastructure to create a KV server.
    Returns a list of services that can be registered with the RPC system.
    """
    kv = MakeKVServer()
    # In the Python version, we return the server instance
    # The test infrastructure will handle RPC registration
    return [kv]


# Test the KV server
def test_kv_server():
    """Test the KV server implementation"""
    print("Testing KV Server...")
    
    # Create server
    server = MakeKVServer()
    
    # Test Get on non-existent key
    get_args = GetArgs("testkey")
    get_reply = GetReply("", 0, "")
    server.Get(get_args, get_reply)
    assert get_reply.Err == Err.ErrNoKey
    print("✓ Get non-existent key returns ErrNoKey")
    
    # Test Put new key with version 0
    put_args = PutArgs("testkey", "value1", 0)
    put_reply = PutReply("")
    server.Put(put_args, put_reply)
    assert put_reply.Err == Err.OK
    print("✓ Put new key with version 0 succeeds")
    
    # Test Get existing key
    get_args = GetArgs("testkey")
    get_reply = GetReply("", 0, "")
    server.Get(get_args, get_reply)
    assert get_reply.Err == Err.OK
    assert get_reply.Value == "value1"
    assert get_reply.Version == 1
    print("✓ Get existing key returns correct value and version")
    
    # Test Put with correct version
    put_args = PutArgs("testkey", "value2", 1)
    put_reply = PutReply("")
    server.Put(put_args, put_reply)
    assert put_reply.Err == Err.OK
    print("✓ Put with correct version succeeds")
    
    # Test Get updated value
    get_args = GetArgs("testkey")
    get_reply = GetReply("", 0, "")
    server.Get(get_args, get_reply)
    assert get_reply.Err == Err.OK
    assert get_reply.Value == "value2"
    assert get_reply.Version == 2
    print("✓ Get returns updated value and incremented version")
    
    # Test Put with wrong version
    put_args = PutArgs("testkey", "value3", 1)  # Should be 2
    put_reply = PutReply("")
    server.Put(put_args, put_reply)
    assert put_reply.Err == Err.ErrVersion
    print("✓ Put with wrong version returns ErrVersion")
    
    # Test Put non-existent key with version > 0
    put_args = PutArgs("newkey", "value1", 1)
    put_reply = PutReply("")
    server.Put(put_args, put_reply)
    assert put_reply.Err == Err.ErrNoKey
    print("✓ Put non-existent key with version > 0 returns ErrNoKey")
    
    print("KV Server test completed successfully!")


if __name__ == "__main__":
    test_kv_server()