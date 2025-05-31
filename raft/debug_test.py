#!/usr/bin/env python3
"""
Simple debug test to check leader election
"""

import time
import logging
from server import TestConfig

# Enable debug logging
logging.basicConfig(level=logging.INFO)

def test_simple_election():
    """Test basic leader election"""
    print("Testing simple leader election...")
    
    config = TestConfig(3, reliable=True, snapshot=False)
    
    try:
        # Wait for initial election
        print("Waiting for initial election...")
        time.sleep(3)
        
        # Check leader
        try:
            leader = config.check_one_leader()
            print(f"✓ Initial leader elected: {leader}")
        except Exception as e:
            print(f"✗ Initial election failed: {e}")
            
            # Debug: Check all server states
            print("\nServer states:")
            for i in range(3):
                try:
                    term, is_leader = config.servers[i].GetState()
                    connected = config._is_connected(i)
                    print(f"  Server {i}: term={term}, leader={is_leader}, connected={connected}")
                except Exception as ex:
                    print(f"  Server {i}: Error getting state: {ex}")
            return
        
        # Test disconnection
        print(f"\nDisconnecting leader {leader}...")
        config.disconnect(leader)
        
        # Wait for new election
        print("Waiting for new election...")
        time.sleep(3)
        
        try:
            new_leader = config.check_one_leader()
            print(f"✓ New leader elected: {new_leader}")
        except Exception as e:
            print(f"✗ Re-election failed: {e}")
            
            # Debug: Check all server states again
            print("\nServer states after disconnection:")
            for i in range(3):
                try:
                    term, is_leader = config.servers[i].GetState()
                    connected = config._is_connected(i)
                    print(f"  Server {i}: term={term}, leader={is_leader}, connected={connected}")
                except Exception as ex:
                    print(f"  Server {i}: Error getting state: {ex}")
        
    finally:
        config.cleanup()
        
def test_network_methods():
    """Test network disconnection methods"""
    print("\nTesting network methods...")
    
    config = TestConfig(3, reliable=True, snapshot=False)
    
    try:
        # Test connectivity
        for i in range(3):
            connected = config._is_connected(i)
            print(f"Server {i} connected: {connected}")
            
        # Test disconnect
        print("\nDisconnecting server 0...")
        config.disconnect(0)
        
        for i in range(3):
            connected = config._is_connected(i)
            print(f"Server {i} connected: {connected}")
            
        # Test reconnect
        print("\nReconnecting server 0...")
        config.connect(0)
        
        for i in range(3):
            connected = config._is_connected(i)
            print(f"Server {i} connected: {connected}")
            
    finally:
        config.cleanup()

if __name__ == "__main__":
    test_network_methods()
    test_simple_election()