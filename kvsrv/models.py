#!/usr/bin/env python3
"""
Porcupine Models for KV Operations
Matching MIT 6.5840 Lab 2 models1 package

This module defines the state machine model for linearizability checking
of key-value operations using the porcupine library.
"""

from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass
import copy

@dataclass
class KvInput:
    """Input for KV operation - matches Go models.KvInput"""
    Op: int        # 0 = Get, 1 = Put
    Key: str       # Key to operate on
    Value: str = ""     # Value for Put operations
    Version: int = 0    # Version for Put operations

@dataclass 
class KvOutput:
    """Output for KV operation - matches Go models.KvOutput"""
    Value: str = ""     # Value returned by Get
    Version: int = 0    # Version returned by Get/Put
    Err: str = ""       # Error code

class KvState:
    """State of the key-value store for linearizability checking"""
    
    def __init__(self):
        # Map from key to (value, version)
        self.data: Dict[str, Tuple[str, int]] = {}
        
    def copy(self) -> 'KvState':
        """Create a copy of the state"""
        new_state = KvState()
        new_state.data = copy.deepcopy(self.data)
        return new_state
        
    def get(self, key: str) -> Tuple[str, int, str]:
        """Get operation on state"""
        if key in self.data:
            value, version = self.data[key]
            return value, version, "OK"
        else:
            return "", 0, "ErrNoKey"
            
    def put(self, key: str, value: str, version: int) -> str:
        """Put operation on state"""
        if key in self.data:
            current_value, current_version = self.data[key]
            if current_version == version:
                # Version matches - update
                self.data[key] = (value, current_version + 1)
                return "OK"
            else:
                # Version mismatch
                return "ErrVersion"
        else:
            # Key doesn't exist
            if version == 0:
                # Create new key
                self.data[key] = (value, 1)
                return "OK"
            else:
                # Key doesn't exist but version > 0
                return "ErrNoKey"

def kv_step(state: Optional[KvState], input_op: KvInput, output: KvOutput) -> Tuple[bool, Optional[KvState]]:
    """
    State transition function for KV operations
    
    Args:
        state: Current state (None if initial state)
        input_op: Input operation
        output: Expected output
        
    Returns:
        (valid, new_state) - whether transition is valid and resulting state
    """
    if state is None:
        state = KvState()
    
    new_state = state.copy()
    
    if input_op.Op == 0:  # Get operation
        expected_value, expected_version, expected_err = new_state.get(input_op.Key)
        
        # Check if output matches expected result
        if (output.Value == expected_value and 
            output.Version == expected_version and 
            output.Err == expected_err):
            return True, new_state
        else:
            return False, None
            
    elif input_op.Op == 1:  # Put operation
        expected_err = new_state.put(input_op.Key, input_op.Value, input_op.Version)
        
        # Check if output matches expected result
        if output.Err == expected_err:
            return True, new_state
        else:
            return False, None
            
    else:
        # Unknown operation
        return False, None

def kv_equal(state1: Optional[KvState], state2: Optional[KvState]) -> bool:
    """Check if two states are equal"""
    if state1 is None and state2 is None:
        return True
    if state1 is None or state2 is None:
        return False
    return state1.data == state2.data

def kv_init() -> KvState:
    """Initialize empty KV state"""
    return KvState()

# Porcupine model definition
class KvModel:
    """Porcupine model for KV operations"""
    
    @staticmethod
    def step(state, input_op, output):
        """Step function for porcupine"""
        return kv_step(state, input_op, output)
        
    @staticmethod
    def equal(state1, state2):
        """Equality function for porcupine"""
        return kv_equal(state1, state2)
        
    @staticmethod
    def init():
        """Initial state for porcupine"""
        return kv_init()

# For compatibility with porcupine library
KvModel = {
    'step': kv_step,
    'equal': kv_equal, 
    'init': kv_init
}

# Test the model
def test_model():
    """Test the KV model implementation"""
    print("Testing KV Model...")
    
    # Test initial state
    state = kv_init()
    assert len(state.data) == 0
    print("✓ Initial state is empty")
    
    # Test Get on empty state
    value, version, err = state.get("nonexistent")
    assert err == "ErrNoKey"
    print("✓ Get non-existent key returns ErrNoKey")
    
    # Test Put new key
    err = state.put("key1", "value1", 0)
    assert err == "OK"
    assert state.data["key1"] == ("value1", 1)
    print("✓ Put new key works")
    
    # Test Get existing key
    value, version, err = state.get("key1")
    assert err == "OK"
    assert value == "value1"
    assert version == 1
    print("✓ Get existing key works")
    
    # Test Put with correct version
    err = state.put("key1", "value2", 1)
    assert err == "OK"
    assert state.data["key1"] == ("value2", 2)
    print("✓ Put with correct version works")
    
    # Test Put with wrong version
    err = state.put("key1", "value3", 1)  # Should be 2
    assert err == "ErrVersion"
    assert state.data["key1"] == ("value2", 2)  # Unchanged
    print("✓ Put with wrong version returns ErrVersion")
    
    # Test Put non-existent key with version > 0
    err = state.put("key2", "value", 1)
    assert err == "ErrNoKey"
    assert "key2" not in state.data
    print("✓ Put non-existent key with version > 0 returns ErrNoKey")
    
    # Test step function
    input_op = KvInput(Op=1, Key="test", Value="val", Version=0)
    output = KvOutput(Err="OK")
    valid, new_state = kv_step(None, input_op, output)
    assert valid
    assert new_state.data["test"] == ("val", 1)
    print("✓ Step function works for Put")
    
    # Test step function for Get
    input_op = KvInput(Op=0, Key="test")
    output = KvOutput(Value="val", Version=1, Err="OK") 
    valid, final_state = kv_step(new_state, input_op, output)
    assert valid
    print("✓ Step function works for Get")
    
    # Test equality
    state1 = kv_init()
    state2 = kv_init()
    assert kv_equal(state1, state2)
    
    state1.put("key", "value", 0)
    assert not kv_equal(state1, state2)
    
    state2.put("key", "value", 0)
    assert kv_equal(state1, state2)
    print("✓ State equality works")
    
    print("KV Model test completed successfully!")

if __name__ == "__main__":
    test_model()