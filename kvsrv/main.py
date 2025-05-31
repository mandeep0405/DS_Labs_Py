#!/usr/bin/env python3
"""
MIT 6.5840 Lab 2: Key/Value Server - Main Runner
Python implementation of the KV server lab

This script demonstrates the KV server and lock functionality,
and provides easy access to run tests.
"""

import sys
import time
import threading
import argparse
from typing import List

# Import all our modules
try:
    from test_config import MakeTestKV
    from client import TestClerk
    from lock import MakeLock
    from rpc import Err
    from kvsrv_test import run_tests
    from enhanced_test import run_enhanced_tests, MakeEnhancedTestKV
    from kvtest import RandValue, MakeKeys, Test, MakeTest
    from porcupine import OpLog
    from annotations import AnnotateInfo, AnnotateSuccess, FinalizeAnnotations, SaveAnnotationsToFile
    import server
    import client
    import lock
    import rpc
    import models
    import porcupine
    import kvtest
    import annotations
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure all files are in the same directory:")
    print("- rpc.py")
    print("- server.py") 
    print("- client.py")
    print("- lock.py")
    print("- test_config.py")
    print("- kvsrv_test.py")
    print("- enhanced_test.py")
    print("- kvtest.py") 
    print("- porcupine.py")
    print("- models.py")
    print("- annotations.py")
    print("- labrpc.py (from previous lab)")
    sys.exit(1)

def demo_basic_kv():
    """Demonstrate basic KV server functionality"""
    print("=== KV Server Basic Demo ===")
    
    # Create test setup
    ts = MakeTestKV(True)
    
    try:
        print("Creating clerk...")
        ck = ts.MakeClerk()
        
        print("\n1. Testing Put operation:")
        err = ck.Put("name", "Alice", 0)
        print(f"   Put('name', 'Alice', 0) -> {err}")
        
        print("\n2. Testing Get operation:")
        value, version, err = ck.Get("name")
        print(f"   Get('name') -> value='{value}', version={version}, err={err}")
        
        print("\n3. Testing Put with correct version:")
        err = ck.Put("name", "Bob", 1)
        print(f"   Put('name', 'Bob', 1) -> {err}")
        
        print("\n4. Testing Get after update:")
        value, version, err = ck.Get("name")
        print(f"   Get('name') -> value='{value}', version={version}, err={err}")
        
        print("\n5. Testing Put with wrong version:")
        err = ck.Put("name", "Charlie", 1)  # Should be 2
        print(f"   Put('name', 'Charlie', 1) -> {err}")
        
        print("\n6. Testing Get non-existent key:")
        value, version, err = ck.Get("nonexistent")
        print(f"   Get('nonexistent') -> value='{value}', version={version}, err={err}")
        
        print("\n7. Testing Put non-existent key with version > 0:")
        err = ck.Put("newkey", "value", 1)
        print(f"   Put('newkey', 'value', 1) -> {err}")
        
    finally:
        ts.Cleanup()
        
    print("\n✓ Basic KV demo completed")

def demo_lock():
    """Demonstrate distributed lock functionality"""
    print("\n=== Distributed Lock Demo ===")
    
    # Create test setup
    ts = MakeTestKV(True)
    
    try:
        print("Creating clerks for two clients...")
        ck1 = ts.MakeClerk()
        ck2 = ts.MakeClerk()
        
        print("Creating locks...")
        lock1 = MakeLock(ck1, "demo_lock")
        lock2 = MakeLock(ck2, "demo_lock")
        
        # Initialize shared counter
        ck1.Put("counter", "0", 0)
        
        def client_worker(client_id: int, lock, clerk: TestClerk, iterations: int):
            """Worker function that uses the lock"""
            for i in range(iterations):
                print(f"   Client {client_id}: Acquiring lock (iteration {i+1})")
                lock.Acquire()
                
                # Read current counter
                value, version, err = clerk.Get("counter")
                if err == Err.OK:
                    current = int(value)
                    new_value = current + 1
                    
                    print(f"   Client {client_id}: Got counter={current}, setting to {new_value}")
                    
                    # Simulate some work
                    time.sleep(0.1)
                    
                    # Update counter
                    err = clerk.Put("counter", str(new_value), version)
                    if err == Err.OK:
                        print(f"   Client {client_id}: Successfully updated counter to {new_value}")
                    else:
                        print(f"   Client {client_id}: Failed to update counter: {err}")
                
                print(f"   Client {client_id}: Releasing lock")
                lock.Release()
                
                # Short delay before next iteration
                time.sleep(0.05)
        
        print("\nStarting concurrent clients with locks...")
        
        # Start two clients concurrently
        thread1 = threading.Thread(target=client_worker, args=(1, lock1, ck1, 3))
        thread2 = threading.Thread(target=client_worker, args=(2, lock2, ck2, 3))
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # Check final counter value
        value, version, err = ck1.Get("counter")
        print(f"\nFinal counter value: {value} (version {version})")
        print("Expected: 6 (both clients did 3 increments each)")
        
    finally:
        ts.Cleanup()
        
    print("\n✓ Lock demo completed")

def demo_unreliable_network():
    """Demonstrate unreliable network handling"""
    print("\n=== Unreliable Network Demo ===")
    
    # Create test setup with unreliable network
    ts = MakeTestKV(False)  # unreliable=False
    
    try:
        print("Creating clerk with unreliable network...")
        ck = ts.MakeClerk()
        
        print("\nTesting Put operations with unreliable network:")
        print("(Some operations may return ErrMaybe due to network issues)")
        
        for i in range(5):
            print(f"\n  Attempt {i+1}:")
            
            # Try to put a value
            err = ck.Put("test", f"value_{i}", i)
            print(f"    Put('test', 'value_{i}', {i}) -> {err}")
            
            if err == Err.ErrMaybe:
                print("    Got ErrMaybe - operation may or may not have succeeded")
                
            # Check what actually happened
            value, version, get_err = ck.Get("test")
            print(f"    Get('test') -> value='{value}', version={version}, err={get_err}")
            
            time.sleep(0.1)
            
    finally:
        ts.Cleanup()
        
    print("\n✓ Unreliable network demo completed")

def demo_enhanced_features():
    """Demonstrate enhanced features including porcupine and kvtest utilities"""
    print("\n=== Enhanced Features Demo ===")
    
    # Create enhanced test setup
    ts = MakeEnhancedTestKV(True)
    test = ts.GetTest()
    
    try:
        print("Creating test with porcupine linearizability checking...")
        AnnotateInfo("Starting enhanced demo", "Testing advanced features")
        
        ck = ts.MakeClerk()
        
        print("\n1. Testing with operation logging:")
        # Operations are automatically logged for linearizability checking
        err = test.Put(ck, "demo", "value1", 0, 0)
        print(f"   Put('demo', 'value1', 0) -> {err}")
        
        value, version, err = test.Get(ck, "demo", 0)
        print(f"   Get('demo') -> value='{value}', version={version}, err={err}")
        
        print("\n2. Testing PutAtLeastOnce:")
        # This handles retries automatically
        final_ver = test.PutAtLeastOnce(ck, "demo", "value2", 1, 0)
        print(f"   PutAtLeastOnce completed with final version: {final_ver}")
        
        print("\n3. Testing JSON operations:")
        from kvtest import EntryV
        entry = EntryV(42, 100)
        err = test.PutJson(ck, "entry", entry, 0, 0)
        print(f"   PutJson(EntryV(42, 100)) -> {err}")
        
        read_entry = EntryV(0, 0)
        ver = test.GetJson(ck, "entry", 0, read_entry)
        print(f"   GetJson() -> Id={read_entry.Id}, V={read_entry.V}, version={ver}")
        
        print("\n4. Testing SpreadPuts:")
        keys, values = test.SpreadPuts(ck, 5)
        print(f"   Created {len(keys)} key-value pairs")
        for i, (key, value) in enumerate(zip(keys[:3], values[:3])):
            print(f"   {key} -> {value[:10]}...")
            
        print("\n5. Checking linearizability:")
        AnnotateInfo("Performing linearizability check", "Verifying operation history")
        test.CheckPorcupine()
        print("   ✓ All operations are linearizable!")
        
        AnnotateSuccess("Enhanced demo completed", "All features working")
        
    finally:
        # Save annotations to file
        annotations = FinalizeAnnotations("Enhanced demo completed")
        SaveAnnotationsToFile(annotations, "enhanced_demo_annotations.html")
        print(f"   📝 Annotations saved to enhanced_demo_annotations.html")
        
        ts.Cleanup()
        
    print("\n✓ Enhanced features demo completed")

def demo_advanced_concurrency():
    """Demonstrate advanced concurrency testing patterns"""
    print("\n=== Advanced Concurrency Demo ===")
    
    ts = MakeEnhancedTestKV(True)
    test = ts.GetTest()
    
    try:
        print("Setting up concurrent client test...")
        AnnotateInfo("Starting concurrency demo", "Testing concurrent operations")
        
        # Initialize shared key
        init_ck = ts.MakeClerk()
        test.Put(init_ck, "shared", "0", 0, -1)
        ts.DeleteClerk(init_ck)
        
        print("\n1. Spawning concurrent clients:")
        # Use advanced SpawnClientsAndWait
        results = test.SpawnClientsAndWait(3, 1.0, test.OneClientPut)
        
        total_ops = sum(r.Nok + r.Nmaybe for r in results)
        print(f"   Total operations: {total_ops}")
        for i, result in enumerate(results):
            print(f"   Client {i}: {result.Nok} OK, {result.Nmaybe} Maybe")
            
        print("\n2. Validating concurrent results:")
        final_ck = ts.MakeClerk()
        from kvtest import ClntRes
        total_res = ClntRes()
        test.CheckPutConcurrent(final_ck, "shared", results, total_res, ts.IsReliable())
        print(f"   ✓ Concurrent operations validated: {total_res.Nok} successful")
        
        print("\n3. Verifying linearizability:")
        test.CheckPorcupine()
        print("   ✓ Concurrent operations are linearizable!")
        
        AnnotateSuccess("Concurrency demo completed", f"Processed {total_ops} concurrent operations")
        
    finally:
        annotations = FinalizeAnnotations("Concurrency demo completed")
        SaveAnnotationsToFile(annotations, "concurrency_demo_annotations.html")
        print(f"   📝 Annotations saved to concurrency_demo_annotations.html")
        
        ts.Cleanup()
        
    print("\n✓ Advanced concurrency demo completed")

def demo_porcupine_standalone():
    """Demonstrate standalone porcupine linearizability checking"""
    print("\n=== Porcupine Linearizability Demo ===")
    
    from porcupine import OpLog, Operation, CheckOperations, CheckResult
    from models import KvModel, KvInput, KvOutput
    
    print("Creating operation log...")
    
    # Create some operations
    operations = [
        Operation(
            Input=KvInput(Op=1, Key="k", Value="A", Version=0),
            Output=KvOutput(Err="OK"),
            Call=1000,
            Return=2000,
            ClientId=1
        ),
        Operation(
            Input=KvInput(Op=0, Key="k"),
            Output=KvOutput(Value="A", Version=1, Err="OK"),
            Call=2500,
            Return=3000,
            ClientId=2
        ),
        Operation(
            Input=KvInput(Op=1, Key="k", Value="B", Version=1),
            Output=KvOutput(Err="OK"),
            Call=3500,
            Return=4000,
            ClientId=1
        )
    ]
    
    print(f"   Created {len(operations)} operations")
    for i, op in enumerate(operations):
        op_type = "Get" if op.Input.Op == 0 else "Put"
        print(f"   Op {i}: Client {op.ClientId} {op_type}")
        
    print("\nChecking linearizability...")
    result = CheckOperations(KvModel, operations)
    print(f"   Result: {result.value}")
    
    if result == CheckResult.OK:
        print("   ✓ Operations are linearizable!")
    else:
        print("   ✗ Operations violate linearizability")
        
    # Test with invalid operations
    print("\nTesting with invalid operations...")
    invalid_ops = [
        Operation(
            Input=KvInput(Op=0, Key="k"),
            Output=KvOutput(Value="wrong", Version=1, Err="OK"),  # Wrong value
            Call=1000,
            Return=2000,
            ClientId=1
        )
    ]
    
    result = CheckOperations(KvModel, invalid_ops)
    print(f"   Result: {result.value}")
    
    if result == CheckResult.ILLEGAL:
        print("   ✓ Invalid operations correctly detected!")
    
    print("\n✓ Porcupine standalone demo completed")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="MIT 6.5840 Lab 2: KV Server")
    parser.add_argument("--test", choices=["basic", "enhanced", "all"], 
                       help="Run test suite")
    parser.add_argument("--demo", choices=[
        "basic", "lock", "unreliable", "enhanced", "concurrency", 
        "porcupine", "all"], 
        default="all", help="Run specific demo")
    
    args = parser.parse_args()
    
    if args.test:
        print("Running tests...")
        if args.test == "basic":
            success = run_tests()
        elif args.test == "enhanced":
            success = run_enhanced_tests()
        else:  # all
            print("=== Running Basic Tests ===")
            success1 = run_tests()
            print("\n=== Running Enhanced Tests ===")
            success2 = run_enhanced_tests()
            success = success1 and success2
        return 0 if success else 1
    else:
        print("MIT 6.5840 Lab 2: Key/Value Server Implementation")
        print("=" * 60)
        print("🚀 Complete implementation with:")
        print("  • Linearizable KV server with versioned operations")
        print("  • Network fault-tolerant client with retry logic")
        print("  • Distributed lock using conditional operations")
        print("  • Porcupine linearizability verification")
        print("  • Advanced testing utilities and patterns")
        print("  • Comprehensive test annotation system")
        print("=" * 60)
        
        if args.demo in ["basic", "all"]:
            demo_basic_kv()
            
        if args.demo in ["lock", "all"]:
            demo_lock()
            
        if args.demo in ["unreliable", "all"]:
            demo_unreliable_network()
            
        if args.demo in ["enhanced", "all"]:
            demo_enhanced_features()
            
        if args.demo in ["concurrency", "all"]:
            demo_advanced_concurrency()
            
        if args.demo in ["porcupine", "all"]:
            demo_porcupine_standalone()
            
        print("\n" + "=" * 60)
        print("🎉 All demos completed!")
        print("\nAvailable commands:")
        print("  python main.py --test basic      # Run basic test suite")
        print("  python main.py --test enhanced   # Run enhanced test suite")  
        print("  python main.py --test all        # Run all tests")
        print("  python main.py --demo enhanced   # Enhanced features demo")
        print("  python main.py --demo concurrency # Concurrency testing demo")
        print("  python main.py --demo porcupine  # Linearizability checking demo")
        print("\n🔬 Features implemented:")
        print("  ✅ All MIT 6.5840 Lab 2 requirements")
        print("  ✅ Porcupine linearizability verification")
        print("  ✅ Advanced test utilities (kvtest.go equivalent)")
        print("  ✅ Test annotation system") 
        print("  ✅ JSON operation support")
        print("  ✅ Concurrent testing patterns")
        print("  ✅ Memory usage validation")
        print("  ✅ Network partition simulation")
        return 0

if __name__ == "__main__":
    sys.exit(main())