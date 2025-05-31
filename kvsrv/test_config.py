#!/usr/bin/env python3
"""
KV Server Test Configuration
Matching MIT 6.5840 Lab 2 test infrastructure

This module provides the test infrastructure for running KV server tests,
including server setup, client management, and network simulation.
"""

import threading
import time
import random
import logging
from typing import List, Dict, Optional, Any, Tuple
import json

# Import our modules
from labrpc import MakeNetwork, MakeServer, MakeService, ClientEnd, Network
from server import KVServer, MakeKVServer
from client import Clerk, MakeClerk, TestClerk
from rpc import Err, Tversion
from porcupine import OpLog
from kvtest import Test, IClerkMaker, MakeTest

logger = logging.getLogger(__name__)

class TestConfig(IClerkMaker):
    """
    Test Configuration for KV Server
    
    Manages server lifecycle, client creation, and test utilities.
    Matches the Go test infrastructure interface and implements IClerkMaker.
    """
    
    def __init__(self, num_servers: int = 1, reliable: bool = True):
        """
        Create test configuration
        
        Args:
            num_servers: Number of servers to create (usually 1 for KV tests)
            reliable: Whether network should be reliable
        """
        self.num_servers = num_servers
        self.reliable = reliable
        
        # Network simulation
        self.net = MakeNetwork()
        self.net.Reliable(reliable)
        
        # Server infrastructure
        self.servers: List[KVServer] = []
        self.rpc_servers: List[Any] = []
        self.client_ends: List[List[ClientEnd]] = []
        
        # Client management
        self.clients: List[Any] = []
        self.client_count = 0
        
        # Statistics
        self.op_count = 0
        
        # Setup servers
        self._setup_servers()
        
    def _setup_servers(self):
        """Setup servers and RPC infrastructure"""
        for i in range(self.num_servers):
            # Create KV server
            server = MakeKVServer()
            self.servers.append(server)
            
            # Create RPC server and service
            rpc_server = MakeServer()
            kv_service = MakeService("KVServer", server)
            rpc_server.AddService(kv_service)
            self.rpc_servers.append(rpc_server)
            
            # Add to network
            self.net.AddServer(i, rpc_server)
            
        # Create client endpoints matrix
        for i in range(self.num_servers):
            row = []
            for j in range(self.num_servers):
                end = self.net.MakeEnd(f"client_{i}_{j}")
                self.net.Connect(f"client_{i}_{j}", j)
                self.net.Enable(f"client_{i}_{j}", True)
                row.append(end)
            self.client_ends.append(row)
            
    def MakeClient(self) -> Any:
        """Create a new client for testing"""
        client_id = self.client_count
        self.client_count += 1
        
        # Create client end for server 0 (single server setup)
        end_name = f"test_client_{client_id}"
        client_end = self.net.MakeEnd(end_name)
        self.net.Connect(end_name, 0)  # Connect to server 0
        self.net.Enable(end_name, True)
        
        # Create mock client object that provides Call method
        class TestClient:
            def __init__(self, client_end: ClientEnd):
                self.client_end = client_end
                
            def Call(self, server: str, method: str, args: Any, reply: Any) -> bool:
                return self.client_end.Call(method, args, reply)
        
        client = TestClient(client_end)
        self.clients.append(client)
        return client
        
    def DeleteClient(self, client: Any):
        """Delete a client"""
        if client in self.clients:
            self.clients.remove(client)
            
    def MakeClerk(self) -> TestClerk:
        """Create a new clerk for testing"""
        client = self.MakeClient()
        clerk = MakeClerk(client, "KVServer")  # Server name doesn't matter for our RPC setup
        return TestClerk(clerk, client)
        
    def DeleteClerk(self, ck: TestClerk):
        """Delete a clerk"""
        self.DeleteClient(ck.client_end)
        
    def Op(self):
        """Increment operation count"""
        self.op_count += 1
        
    def GetOpCount(self) -> int:
        """Get total operation count"""
        return self.op_count
        
    def GetRPCCount(self) -> int:
        """Get total RPC count"""
        return self.net.GetTotalCount()
        
    def IsReliable(self) -> bool:
        """Check if network is reliable"""
        return self.reliable
        
    def SetReliable(self, reliable: bool):
        """Set network reliability"""
        self.reliable = reliable
        self.net.Reliable(reliable)
        
    def End(self):
        """End the test configuration"""
        pass
        
    def Cleanup(self):
        """Cleanup test resources"""
        for server in self.servers:
            server.Kill()
        self.net.Cleanup()


class TestKV:
    """
    Main test class for KV server tests
    Matches the Go TestKV structure
    """
    
    def __init__(self, reliable: bool = True):
        """Create test instance"""
        self.config = TestConfig(1, reliable)  # Single server for KV tests
        self.reliable = reliable
        
    def MakeClerk(self) -> TestClerk:
        """Create a new clerk"""
        return self.config.MakeClerk()
        
    def DeleteClerk(self, ck: TestClerk):
        """Delete a clerk"""
        self.config.DeleteClient(ck.client_end)
        
    def Begin(self, description: str):
        """Begin a test with description"""
        print(f"Test: {description}")
        
    def Cleanup(self):
        """Cleanup test resources"""
        self.config.Cleanup()
        
    def IsReliable(self) -> bool:
        """Check if network is reliable"""
        return self.reliable
        
    def SetReliable(self, reliable: bool):
        """Set network reliability"""
        self.reliable = reliable
        self.config.SetReliable(reliable)


def MakeTestKV(reliable: bool = True) -> TestKV:
    """Create a test KV instance"""
    return TestKV(reliable)


# Utility functions for testing
def RandValue(n: int) -> str:
    """Generate random string of length n"""
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join(random.choice(letters) for _ in range(n))

def PutJson(ck: TestClerk, key: str, value: Any, version: Tversion) -> str:
    """Put JSON-encoded value"""
    json_value = json.dumps(value)
    return ck.Put(key, json_value, version)

def GetJson(ck: TestClerk, key: str, default_value: Any) -> Tuple[Tversion, Any]:
    """Get and JSON-decode value"""
    value, version, err = ck.Get(key)
    if err == Err.OK:
        try:
            decoded = json.loads(value)
            return version, decoded
        except json.JSONDecodeError:
            return version, default_value
    else:
        return 0, default_value


# Test the configuration
def test_config():
    """Test the test configuration"""
    print("Testing KV Test Configuration...")
    
    # Create test config
    ts = MakeTestKV(True)
    
    try:
        # Create clerk
        ck = ts.MakeClerk()
        
        # Test basic operations
        err = ck.Put("test", "value", 0)
        assert err == Err.OK
        print("✓ Put operation works")
        
        value, version, err = ck.Get("test")
        assert err == Err.OK
        assert value == "value"
        assert version == 1
        print("✓ Get operation works")
        
        # Test error cases
        value, version, err = ck.Get("nonexistent")
        assert err == Err.ErrNoKey
        print("✓ Get non-existent key returns ErrNoKey")
        
        err = ck.Put("test", "newvalue", 0)  # Wrong version
        assert err == Err.ErrVersion
        print("✓ Put with wrong version returns ErrVersion")
        
        print("Test configuration works correctly!")
        
    finally:
        ts.Cleanup()


if __name__ == "__main__":
    test_config()