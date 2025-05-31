#!/usr/bin/env python3
"""
Enhanced Persister - Python Implementation
Exactly matching Go lab persister.go behavior

This module provides persistent storage simulation for Raft state
and snapshots, matching the MIT 6.5840 lab requirements.
"""

import threading
import copy
from typing import Optional

class Persister:
    """
    Persistent storage simulation - exactly matching Go lab Persister behavior
    
    Simulates persistent storage for Raft state and snapshots.
    Thread-safe operations with proper copying to prevent data races.
    """
    
    def __init__(self):
        """Create persister"""
        self.mu = threading.Lock()
        self.raftstate = b''  # Persistent Raft state
        self.snapshot = b''   # Snapshot data
        
    def Copy(self) -> 'Persister':
        """Create a copy of persister - matching Go lab Copy()"""
        with self.mu:
            np = Persister()
            np.raftstate = self.raftstate
            np.snapshot = self.snapshot
            return np
            
    def ReadRaftState(self) -> bytes:
        """Read Raft state - matching Go lab ReadRaftState()"""
        with self.mu:
            return self._clone(self.raftstate)
            
    def RaftStateSize(self) -> int:
        """Get size of Raft state - matching Go lab RaftStateSize()"""
        with self.mu:
            return len(self.raftstate)
            
    def Save(self, raftstate: bytes, snapshot: bytes):
        """
        Save both Raft state and snapshot atomically - matching Go lab Save()
        
        This is the main persistence interface used by Raft.
        """
        with self.mu:
            self.raftstate = self._clone(raftstate) if raftstate else b''
            self.snapshot = self._clone(snapshot) if snapshot else b''
            
    def SaveRaftState(self, raftstate: bytes):
        """
        Save only Raft state - REQUIRED by raft.py
        
        This method is called by raft.persist() and must exist.
        """
        with self.mu:
            self.raftstate = self._clone(raftstate) if raftstate else b''
            
    def ReadSnapshot(self) -> bytes:
        """Read snapshot data - matching Go lab ReadSnapshot()"""
        with self.mu:
            return self._clone(self.snapshot)
            
    def SnapshotSize(self) -> int:
        """Get size of snapshot - matching Go lab SnapshotSize()"""
        with self.mu:
            return len(self.snapshot)
            
    def _clone(self, data: bytes) -> bytes:
        """Create a copy of byte data to prevent races"""
        if data is None:
            return b''
        return bytes(data)  # Create new bytes object


def MakePersister() -> Persister:
    """Create a new persister - matching Go lab MakePersister()"""
    return Persister()


# Test the persister
def test_persister():
    """Test the persister functionality"""
    print("Testing Enhanced Persister...")
    
    # Create persister
    p = MakePersister()
    
    # Test empty state
    assert p.ReadRaftState() == b''
    assert p.ReadSnapshot() == b''
    assert p.RaftStateSize() == 0
    assert p.SnapshotSize() == 0
    print("✓ Empty state test passed")
    
    # Test saving Raft state
    test_state = b'test raft state data'
    p.SaveRaftState(test_state)
    
    assert p.ReadRaftState() == test_state
    assert p.RaftStateSize() == len(test_state)
    print("✓ Raft state save/read test passed")
    
    # Test saving both state and snapshot
    test_snapshot = b'test snapshot data'
    p.Save(test_state, test_snapshot)
    
    assert p.ReadRaftState() == test_state
    assert p.ReadSnapshot() == test_snapshot
    assert p.RaftStateSize() == len(test_state)
    assert p.SnapshotSize() == len(test_snapshot)
    print("✓ Combined save test passed")
    
    # Test copy functionality
    p2 = p.Copy()
    assert p2.ReadRaftState() == test_state
    assert p2.ReadSnapshot() == test_snapshot
    
    # Modify original - copy should be unchanged
    p.SaveRaftState(b'modified state')
    assert p2.ReadRaftState() == test_state  # Should be unchanged
    print("✓ Copy test passed")
    
    # Test thread safety
    import threading
    import time
    
    def writer_thread(persister, data_prefix):
        for i in range(10):
            data = f'{data_prefix}_{i}'.encode()
            persister.SaveRaftState(data)
            time.sleep(0.001)
            
    def reader_thread(persister, results):
        for i in range(10):
            state = persister.ReadRaftState()
            results.append(len(state))
            time.sleep(0.001)
    
    p3 = MakePersister()
    results = []
    
    # Start concurrent threads
    t1 = threading.Thread(target=writer_thread, args=(p3, 'thread1'))
    t2 = threading.Thread(target=writer_thread, args=(p3, 'thread2'))
    t3 = threading.Thread(target=reader_thread, args=(p3, results))
    
    t1.start()
    t2.start()
    t3.start()
    
    t1.join()
    t2.join()
    t3.join()
    
    # Should have completed without crashes
    assert len(results) == 10
    print("✓ Thread safety test passed")
    
    # Test SaveRaftState specifically (the missing method)
    p4 = MakePersister()
    test_data = b'specific saveraftstate test'
    p4.SaveRaftState(test_data)
    assert p4.ReadRaftState() == test_data
    print("✓ SaveRaftState method test passed")
    
    print("Enhanced Persister test completed successfully!")


if __name__ == "__main__":
    test_persister()