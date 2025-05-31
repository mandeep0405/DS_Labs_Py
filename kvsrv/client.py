#!/usr/bin/env python3
"""
KV Client Implementation (Clerk)
Matching MIT 6.5840 Lab 2 kvsrv1/client.go

This implements the client-side logic for communicating with the KV server,
including retry logic for handling network failures and proper ErrMaybe semantics.
"""

import time
import logging
from typing import Tuple, Optional

# Import our RPC types and labrpc
from rpc import Err, GetArgs, GetReply, PutArgs, PutReply, Tversion

logger = logging.getLogger(__name__)

class Clerk:
    """
    KV Client (Clerk) Implementation
    
    Provides Put and Get methods that handle network failures through retries.
    Implements proper at-most-once semantics for Put operations.
    """
    
    def __init__(self, client_end, server_name: str):
        """
        Create a new Clerk
        
        Args:
            client_end: Client object that provides Call method (from test framework)
            server_name: Name of the server to send RPCs to
        """
        self.clnt = client_end
        self.server = server_name
        
    def Get(self, key: str) -> Tuple[str, Tversion, str]:
        """
        Get the current value and version for a key
        
        Returns:
            (value, version, error) - error is Err.OK or Err.ErrNoKey
        """
        args = GetArgs(key)
        
        while True:
            reply = GetReply("", 0, "")
            ok = self.clnt.Call(self.server, "KVServer.Get", args, reply)
            
            if ok:
                # Successfully received reply from server
                return reply.Value, reply.Version, reply.Err
            
            # RPC failed, retry after short delay
            time.sleep(0.01)  # 10ms delay before retry
    
    def Put(self, key: str, value: str, version: Tversion) -> str:
        """
        Put a key-value pair with version control
        
        Args:
            key: Key to update
            value: New value
            version: Expected current version of the key
            
        Returns:
            Error code: Err.OK, Err.ErrVersion, Err.ErrNoKey, or Err.ErrMaybe
        """
        args = PutArgs(key, value, version)
        
        # Track if this is a retry to handle ErrMaybe semantics
        is_retry = False
        
        while True:
            reply = PutReply("")
            ok = self.clnt.Call(self.server, "KVServer.Put", args, reply)
            
            if ok:
                # Successfully received reply from server
                if reply.Err == Err.ErrVersion and is_retry:
                    # This is a retry and got ErrVersion - the first attempt might have succeeded
                    # Return ErrMaybe since we can't know if the operation was executed
                    return Err.ErrMaybe
                else:
                    # Return the actual error from the server
                    return reply.Err
            
            # RPC failed, will retry
            is_retry = True
            time.sleep(0.01)  # 10ms delay before retry


class TestClerk:
    """
    Test wrapper for Clerk to match the Go test infrastructure interface
    """
    
    def __init__(self, clerk: Clerk, client_end):
        """Create test clerk wrapper"""
        self.clerk = clerk
        self.client_end = client_end
        
    def Get(self, key: str) -> Tuple[str, Tversion, str]:
        """Get method for test interface"""
        return self.clerk.Get(key)
        
    def Put(self, key: str, value: str, version: Tversion) -> str:
        """Put method for test interface"""
        return self.clerk.Put(key, value, version)


def MakeClerk(client_end, server_name: str) -> Clerk:
    """
    Create a new Clerk instance
    
    Args:
        client_end: Client object from test framework
        server_name: Name of the server to connect to
        
    Returns:
        New Clerk instance
    """
    return Clerk(client_end, server_name)


# Test the clerk implementation
def test_clerk():
    """Test the Clerk implementation with a mock client"""
    print("Testing KV Clerk...")
    
    # Mock client for testing
    class MockClient:
        def __init__(self):
            self.call_count = 0
            self.should_fail = False
            
        def Call(self, server: str, method: str, args, reply):
            self.call_count += 1
            
            if self.should_fail and self.call_count == 1:
                # Simulate network failure on first attempt
                return False
                
            # Simulate successful responses
            if method == "KVServer.Get":
                if args.Key == "existing":
                    reply.Value = "test_value"
                    reply.Version = 1
                    reply.Err = Err.OK
                else:
                    reply.Value = ""
                    reply.Version = 0
                    reply.Err = Err.ErrNoKey
            elif method == "KVServer.Put":
                if args.Key == "test" and args.Version == 0:
                    reply.Err = Err.OK
                else:
                    reply.Err = Err.ErrVersion
                    
            return True
    
    # Test basic functionality
    mock_client = MockClient()
    clerk = Clerk(mock_client, "test_server")
    
    # Test Get existing key
    value, version, err = clerk.Get("existing")
    assert err == Err.OK
    assert value == "test_value"
    assert version == 1
    print("✓ Get existing key works")
    
    # Test Get non-existent key
    value, version, err = clerk.Get("nonexistent")
    assert err == Err.ErrNoKey
    print("✓ Get non-existent key returns ErrNoKey")
    
    # Test Put new key
    err = clerk.Put("test", "value", 0)
    assert err == Err.OK
    print("✓ Put new key works")
    
    # Test Put with wrong version
    err = clerk.Put("test", "value", 1)
    assert err == Err.ErrVersion
    print("✓ Put with wrong version returns ErrVersion")
    
    # Test retry logic
    mock_client.should_fail = True
    mock_client.call_count = 0
    value, version, err = clerk.Get("existing")
    assert err == Err.OK
    assert mock_client.call_count == 2  # Should have retried
    print("✓ Retry logic works for Get")
    
    # Test ErrMaybe logic
    mock_client.call_count = 0
    err = clerk.Put("test", "value", 1)  # Will fail first time, then get ErrVersion
    assert err == Err.ErrMaybe  # Should return ErrMaybe for retry with ErrVersion
    print("✓ ErrMaybe logic works for Put retries")
    
    print("Clerk test completed successfully!")


if __name__ == "__main__":
    test_clerk()