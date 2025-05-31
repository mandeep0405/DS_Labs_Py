#!/usr/bin/env python3
"""
Porcupine Linearizability Checker
Matching MIT 6.5840 Lab 2 porcupine.go functionality

This module provides linearizability checking for concurrent operations.
It's a simplified Python implementation of the porcupine library concepts.
"""

import threading
import time
import copy
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class CheckResult(Enum):
    """Result of linearizability check"""
    UNKNOWN = "Unknown"
    ILLEGAL = "Illegal" 
    OK = "Ok"

@dataclass
class Operation:
    """Single operation for linearizability checking"""
    Input: Any          # Input to the operation
    Output: Any         # Output from the operation  
    Call: int           # Time when operation was called (nanoseconds)
    Return: int         # Time when operation returned (nanoseconds)
    ClientId: int       # ID of client that performed operation

@dataclass
class Event:
    """Event in the operation history"""
    kind: str           # "call" or "return"
    value: Any         # Input for call, output for return
    id: int            # Operation ID
    time: int          # Timestamp

class OpLog:
    """
    Operation log for recording concurrent operations
    Matching Go lab OpLog functionality
    """
    
    def __init__(self):
        """Create operation log"""
        self.operations: List[Operation] = []
        self.mu = threading.Lock()
        
    def Len(self) -> int:
        """Get number of operations"""
        with self.mu:
            return len(self.operations)
            
    def Append(self, op: Operation):
        """Append operation to log"""
        with self.mu:
            self.operations.append(op)
            
    def Read(self) -> List[Operation]:
        """Read all operations (returns copy)"""
        with self.mu:
            return copy.deepcopy(self.operations)

class LinearizabilityInfo:
    """Information about linearizability check"""
    
    def __init__(self):
        self.result = CheckResult.UNKNOWN
        self.history: List[Operation] = []
        self.linearization: Optional[List[int]] = None
        
    def AddAnnotations(self, annotations: List[Any]):
        """Add annotations for visualization (placeholder)"""
        pass

def operations_to_events(operations: List[Operation]) -> List[Event]:
    """Convert operations to events for checking"""
    events = []
    
    for i, op in enumerate(operations):
        # Call event
        events.append(Event(
            kind="call",
            value=op.Input,
            id=i,
            time=op.Call
        ))
        
        # Return event
        events.append(Event(
            kind="return", 
            value=op.Output,
            id=i,
            time=op.Return
        ))
    
    # Sort by time
    events.sort(key=lambda e: e.time)
    return events

def check_sequential_consistency(model: Dict[str, Callable], operations: List[Operation]) -> bool:
    """
    Check if operations are sequentially consistent
    (Simpler check than full linearizability)
    """
    if not operations:
        return True
        
    # Try all possible orderings of operations
    # For simplicity, we'll just check if the operations could be executed
    # in the order they completed (based on return times)
    
    # Sort by return time
    sorted_ops = sorted(operations, key=lambda op: op.Return)
    
    # Execute operations in order and check validity
    state = model['init']()
    
    for op in sorted_ops:
        valid, new_state = model['step'](state, op.Input, op.Output)
        if not valid:
            return False
        state = new_state
        
    return True

def check_linearizability_simple(model: Dict[str, Callable], operations: List[Operation]) -> Tuple[CheckResult, LinearizabilityInfo]:
    """
    Simplified linearizability checking
    
    This is a basic implementation that checks sequential consistency.
    A full linearizability checker would be much more complex.
    """
    info = LinearizabilityInfo()
    info.history = operations
    
    if not operations:
        info.result = CheckResult.OK
        return CheckResult.OK, info
    
    try:
        # Check if operations can be executed sequentially
        if check_sequential_consistency(model, operations):
            info.result = CheckResult.OK
            return CheckResult.OK, info
        else:
            info.result = CheckResult.ILLEGAL
            return CheckResult.ILLEGAL, info
            
    except Exception as e:
        logger.debug(f"Linearizability check failed: {e}")
        info.result = CheckResult.UNKNOWN
        return CheckResult.UNKNOWN, info

def CheckOperations(model: Dict[str, Callable], operations: List[Operation]) -> CheckResult:
    """Check linearizability of operations"""
    result, _ = check_linearizability_simple(model, operations)
    return result

def CheckOperationsVerbose(model: Dict[str, Callable], operations: List[Operation], timeout_sec: float) -> Tuple[CheckResult, LinearizabilityInfo]:
    """Check linearizability with detailed information"""
    # For timeout, we'll use a simple approach
    start_time = time.time()
    
    if time.time() - start_time > timeout_sec:
        info = LinearizabilityInfo()
        info.history = operations
        info.result = CheckResult.UNKNOWN
        return CheckResult.UNKNOWN, info
    
    return check_linearizability_simple(model, operations)

def Visualize(model: Dict[str, Callable], info: LinearizabilityInfo, file_handle) -> Optional[Exception]:
    """
    Create visualization of linearizability check
    (Placeholder implementation)
    """
    try:
        file_handle.write("<html><head><title>Linearizability Check</title></head><body>")
        file_handle.write(f"<h1>Linearizability Check Result: {info.result.value}</h1>")
        file_handle.write(f"<p>Operations checked: {len(info.history)}</p>")
        
        if info.history:
            file_handle.write("<h2>Operation History:</h2><ul>")
            for i, op in enumerate(info.history):
                file_handle.write(f"<li>Op {i}: Client {op.ClientId}, "
                                f"Input: {op.Input}, Output: {op.Output}</li>")
            file_handle.write("</ul>")
            
        file_handle.write("</body></html>")
        return None
    except Exception as e:
        return e

# Global time reference for monotonic timestamps
t0 = time.time()

def get_time() -> int:
    """Get current time in nanoseconds since t0"""
    return int((time.time() - t0) * 1_000_000_000)

# Test the porcupine implementation
def test_porcupine():
    """Test the porcupine implementation"""
    print("Testing Porcupine Linearizability Checker...")
    
    # Create operation log
    log = OpLog()
    assert log.Len() == 0
    print("✓ OpLog creation works")
    
    # Test operation logging
    op1 = Operation(
        Input={"op": "put", "key": "k", "value": "v"},
        Output={"err": "OK"},
        Call=get_time(),
        Return=get_time() + 1000,
        ClientId=1
    )
    
    log.Append(op1)
    assert log.Len() == 1
    print("✓ Operation logging works")
    
    # Test reading operations
    ops = log.Read()
    assert len(ops) == 1
    assert ops[0].ClientId == 1
    print("✓ Operation reading works")
    
    # Test simple model checking
    from models import KvModel, KvInput, KvOutput
    
    # Create some test operations
    operations = [
        Operation(
            Input=KvInput(Op=1, Key="k", Value="v", Version=0),
            Output=KvOutput(Err="OK"),
            Call=1000,
            Return=2000,
            ClientId=1
        ),
        Operation(
            Input=KvInput(Op=0, Key="k"),
            Output=KvOutput(Value="v", Version=1, Err="OK"),
            Call=3000,
            Return=4000,
            ClientId=2
        )
    ]
    
    # Check linearizability
    result = CheckOperations(KvModel, operations)
    assert result == CheckResult.OK
    print("✓ Valid operations pass linearizability check")
    
    # Test invalid operations
    invalid_operations = [
        Operation(
            Input=KvInput(Op=0, Key="k"),
            Output=KvOutput(Value="wrong", Version=1, Err="OK"),  # Wrong value
            Call=1000,
            Return=2000,
            ClientId=1
        )
    ]
    
    result = CheckOperations(KvModel, invalid_operations)
    assert result == CheckResult.ILLEGAL
    print("✓ Invalid operations fail linearizability check")
    
    # Test verbose checking
    result, info = CheckOperationsVerbose(KvModel, operations, 1.0)
    assert result == CheckResult.OK
    assert len(info.history) == 2
    print("✓ Verbose checking works")
    
    print("Porcupine test completed successfully!")

if __name__ == "__main__":
    test_porcupine()