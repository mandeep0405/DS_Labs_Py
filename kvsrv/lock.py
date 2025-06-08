#!/usr/bin/env python3
"""
Distributed Lock Implementation 
Matching MIT 6.5840 Lab 2 kvsrv1/lock/lock.go

This implements a distributed lock using the KV server's conditional Put operations.
The lock uses a key to track lock state and ensures mutual exclusion across clients.

"""

import time
import logging
from typing_extensions import Protocol
from typing import Tuple
from rpc import Err, Tversion

logger = logging.getLogger(__name__)

class IKVClerk(Protocol):
    """Interface for KV clerk - matches Go interface"""
    def Get(self, key: str) -> Tuple[str, Tversion, str]:
        """Get value and version for key"""
        ...
        
    def Put(self, key: str, value: str, version: Tversion) -> str:
        """Put value with version control"""
        ...

class Lock:
    """
    Distributed Lock Implementation 
    
    Uses conditional Put operations on a KV server to implement mutual exclusion.
    Only one client can hold the lock at a time.
    
    """
    
    def __init__(self, ck: IKVClerk, lock_key: str):
        """
        Create a new Lock
        
        Args:
            ck: KV clerk interface for communicating with server
            lock_key: Key to use for storing lock state
        """
        self.ck = ck
        self.lock_key = lock_key
        self.client_id = f"client_{id(self)}"  # Unique client ID
        
    def Acquire(self):
        """
        Acquire the lock
        
        Blocks until the lock is successfully acquired.
        Uses conditional Put to atomically acquire the lock with proper ErrMaybe handling.
        """
        while True:
            # First, try to create lock with version 0 (for new locks)
            err = self.ck.Put(self.lock_key, self.client_id, 0)
            
            if err == Err.OK:
                # Successfully acquired new lock
                return
            elif err == Err.ErrMaybe:
                # FIXED: Don't assume we got the lock on ErrMaybe
                # We need to check if WE specifically set the lock
                value, version, get_err = self.ck.Get(self.lock_key)
                if get_err == Err.OK and value == self.client_id:
                    # Lock exists and is set to OUR client ID - we acquired it
                    return
                # Otherwise, we didn't get it, continue trying
            elif err == Err.ErrVersion:
                # Lock key exists - check if it's available (released)
                value, version, get_err = self.ck.Get(self.lock_key)
                if get_err == Err.OK:
                    if value == "":
                        # Lock is released (empty value) - try to acquire with correct version
                        acquire_err = self.ck.Put(self.lock_key, self.client_id, version)
                        if acquire_err == Err.OK:
                            # Successfully acquired released lock
                            return
                        elif acquire_err == Err.ErrMaybe:
                            # FIXED: Check if WE specifically set the lock
                            check_value, _, check_err = self.ck.Get(self.lock_key)
                            if check_err == Err.OK and check_value == self.client_id:
                                # We acquired it
                                return
                        # If ErrVersion or we didn't get it, someone else got it first - continue trying
                    # If value != "", someone else holds the lock - wait and retry
                else:
                    # Get failed, retry
                    pass
            elif err == Err.ErrNoKey:
                # This shouldn't happen when putting with version 0, but continue
                pass
                
            # Wait a bit before retrying to avoid busy spinning
            time.sleep(0.01)  # 10ms delay
            
    def Release(self):
        """
        Release the lock 
        
        Resets the lock by setting it to empty string with the current version.
        This allows other clients to acquire the lock.
        """
        while True:
            # Get current lock state
            value, version, err = self.ck.Get(self.lock_key)
            
            if err == Err.ErrNoKey:
                # Lock doesn't exist - already released
                return
            elif err == Err.OK:
                # Check if we actually hold the lock
                if value != self.client_id and value != "":
                    # Someone else holds the lock - we can't release it
                    logger.warning(f"Attempted to release lock held by {value}, we are {self.client_id}")
                    return
                    
                # Lock exists and either we hold it or it's empty - try to release it
                release_err = self.ck.Put(self.lock_key, "", version)
                
                if release_err == Err.OK:
                    # Successfully released
                    return
                elif release_err == Err.ErrMaybe:
                    # Might have released - check if lock is now empty or someone else has it
                    check_value, _, check_err = self.ck.Get(self.lock_key)
                    if check_err == Err.ErrNoKey or (check_err == Err.OK and check_value == ""):
                        # Lock is released
                        return
                    elif check_err == Err.OK and check_value != self.client_id:
                        # Someone else has the lock now - assume we released it
                        return
                    # Otherwise retry
                elif release_err == Err.ErrVersion:
                    # Version changed - retry with new version
                    continue
                    
            # Wait a bit before retrying
            time.sleep(0.01)  # 10ms delay


def MakeLock(ck: IKVClerk, lock_key: str) -> Lock:
    """
    Create a new Lock instance
    
    Args:
        ck: KV clerk for server communication
        lock_key: Key to use for lock state
        
    Returns:
        New Lock instance
    """
    return Lock(ck, lock_key)


# Test the fixed lock implementation
def test_lock():
    """Test the Lock implementation with a mock KV clerk"""
    print("Testing Distributed Lock...")
    
    class MockKVClerk:
        """Mock KV clerk for testing lock logic"""
        
        def __init__(self):
            self.data = {}  # key -> (value, version)
            self.put_count = 0
            
        def Get(self, key: str) -> Tuple[str, Tversion, str]:
            if key in self.data:
                value, version = self.data[key]
                return value, version, Err.OK
            else:
                return "", 0, Err.ErrNoKey
                
        def Put(self, key: str, value: str, version: Tversion) -> str:
            self.put_count += 1
            
            if key in self.data:
                current_value, current_version = self.data[key]
                if current_version == version:
                    # Version matches - update
                    self.data[key] = (value, current_version + 1)
                    return Err.OK
                else:
                    # Version mismatch
                    return Err.ErrVersion
            else:
                # Key doesn't exist
                if version == 0:
                    # Create new key
                    self.data[key] = (value, 1)
                    return Err.OK
                else:
                    # Key doesn't exist but version > 0
                    return Err.ErrNoKey
    
    # Test basic lock operations
    mock_clerk = MockKVClerk()
    lock1 = MakeLock(mock_clerk, "test_lock")
    lock2 = MakeLock(mock_clerk, "test_lock")
    
    # Initially lock should not exist
    value, version, err = mock_clerk.Get("test_lock")
    assert err == Err.ErrNoKey
    print("✓ Lock initially doesn't exist")
    
    # Acquire lock with client 1
    lock1.Acquire()
    value, version, err = mock_clerk.Get("test_lock")
    assert err == Err.OK
    assert value == lock1.client_id
    assert version == 1
    print("✓ Lock acquire works")
    
    # Try to acquire with client 2 (should fail in real scenario, but we'll simulate)
    # Second acquire should fail because lock exists with different client ID
    err = mock_clerk.Put("test_lock", lock2.client_id, 0)
    assert err == Err.ErrVersion  # Lock already exists
    print("✓ Second acquire fails when lock held by different client")
    
    # Release lock with client 1
    lock1.Release()
    value, version, err = mock_clerk.Get("test_lock")
    assert err == Err.OK
    assert value == ""  # Lock is released (empty value)
    assert version == 2  # Version incremented
    print("✓ Lock release works")
    
    # Should be able to acquire with client 2 after release
    lock2.Acquire()
    value, version, err = mock_clerk.Get("test_lock")
    assert err == Err.OK
    assert value == lock2.client_id
    assert version == 3  # Version should increment
    print("✓ Can acquire lock after release")
    
    # Clean up - release the lock
    lock2.Release()
    print("✓ Final release successful")
    
    print("Lock test completed successfully!")


if __name__ == "__main__":
    test_lock()