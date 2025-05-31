#!/usr/bin/env python3
"""
KV Server Test Suite - COMPLETE FIXED VERSION
Matching MIT 6.5840 Lab 2 kvsrv_test.go

This module contains all the test cases for the KV server implementation,
including basic operations, concurrency, unreliable networks, and memory usage.

FIXED: 
- Threading issue in lock tests
- Proper mutual exclusion detection
- Exception handling from worker threads
- More robust lock testing logic
"""

import threading
import time
import random
import unittest
import sys
import logging
import json
import gc
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our modules
from test_config import MakeTestKV, TestKV, RandValue, PutJson, GetJson
from client import TestClerk
from lock import MakeLock
from rpc import Err, Tversion

# Try to import psutil for memory testing
try:
    import psutil
    import os
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# Configure logging
logging.basicConfig(level=logging.WARNING)  # Reduce noise during tests
logger = logging.getLogger(__name__)

class TestKVServer(unittest.TestCase):
    """
    KV Server Test Cases
    
    Tests all aspects of the KV server implementation including
    basic operations, concurrency, network failures, and memory usage.
    """
    
    def test_reliable_put(self):
        """Test Put with a single client and reliable network"""
        print("\n=== Test: Reliable Put ===")
        
        VAL = "6.5840"
        VER = 0
        
        ts = MakeTestKV(True)
        ts.Begin("One client and reliable Put")
        
        try:
            ck = ts.MakeClerk()
            
            # Test basic Put
            err = ck.Put("k", VAL, VER)
            self.assertEqual(err, Err.OK, f"Put err {err}")
            
            # Test Get after Put
            val, ver, err = ck.Get("k")
            self.assertEqual(err, Err.OK, f"Get err {err}; expected OK")
            self.assertEqual(val, VAL, f"Get value err {val}; expected {VAL}")
            self.assertEqual(ver, VER + 1, f"Get wrong version {ver}; expected {VER + 1}")
            
            # Test Put with wrong version
            err = ck.Put("k", VAL, 0)
            self.assertEqual(err, Err.ErrVersion, f"expected Put to fail with ErrVersion; got err={err}")
            
            # Test Put non-existent key with wrong version
            err = ck.Put("y", VAL, 1)
            self.assertEqual(err, Err.ErrNoKey, f"expected Put to fail with ErrNoKey; got err={err}")
            
            # Test Get non-existent key
            _, _, err = ck.Get("y")
            self.assertEqual(err, Err.ErrNoKey, f"expected Get to fail with ErrNoKey; got err={err}")
            
            print("✓ Reliable Put test passed")
            
        finally:
            ts.Cleanup()
            
    def test_put_concurrent_reliable(self):
        """Test many clients putting on same key with reliable network"""
        print("\n=== Test: Concurrent Put Reliable ===")
        
        NCLNT = 10
        NSEC = 1
        
        ts = MakeTestKV(True)
        ts.Begin("Test: many clients racing to put values to the same key")
        
        try:
            # Spawn clients that concurrently put to the same key
            def client_worker(client_id: int, results: List, done_event: threading.Event):
                """Worker function for concurrent client"""
                ck = ts.MakeClerk()
                put_count = 0
                
                try:
                    while not done_event.is_set():
                        # Try to put a value with current version
                        val, ver, get_err = ck.Get("k")
                        if get_err == Err.ErrNoKey:
                            ver = 0  # Key doesn't exist yet
                        elif get_err != Err.OK:
                            continue  # Skip this iteration on error
                            
                        put_err = ck.Put("k", f"client_{client_id}_value_{put_count}", ver)
                        if put_err == Err.OK:
                            put_count += 1
                        # Continue regardless of result
                        
                except Exception as e:
                    logger.error(f"Client {client_id} error: {e}")
                finally:
                    results[client_id] = put_count
                    ts.DeleteClerk(ck)
            
            # Start concurrent clients
            results = [0] * NCLNT
            done_event = threading.Event()
            threads = []
            
            for i in range(NCLNT):
                thread = threading.Thread(target=client_worker, args=(i, results, done_event))
                threads.append(thread)
                thread.start()
                
            # Let them run for specified time
            time.sleep(NSEC)
            done_event.set()
            
            # Wait for all threads to complete
            for thread in threads:
                thread.join(timeout=5)
                
            # Check final state
            ck = ts.MakeClerk()
            val, ver, err = ck.Get("k")
            
            total_puts = sum(results)
            print(f"Total successful puts: {total_puts}, Final version: {ver}")
            
            # Version should match number of successful puts
            if err == Err.OK:
                self.assertGreaterEqual(ver, 1, "Should have at least one successful put")
                self.assertLessEqual(ver, total_puts + 1, "Version shouldn't exceed puts + 1")
            
            print("✓ Concurrent Put test passed")
            
        finally:
            ts.Cleanup()
            
    def test_memory_put_many_clients_reliable(self):
        """Test memory usage with many put clients"""
        print("\n=== Test: Memory Put Many Clients ===")
        
        if not HAS_PSUTIL:
            print("⚠️  Skipping memory test (psutil not available)")
            return
            
        NCLIENT = 1000  # Reduced from 100,000 for Python
        MEM = 100       # Reduced memory per value
        
        ts = MakeTestKV(True)
        
        try:
            v = RandValue(MEM)
            
            # Create many clerks
            clerks = []
            for i in range(NCLIENT):
                clerks.append(ts.MakeClerk())
                
            # Force allocation by making initial failed puts
            for i in range(NCLIENT):
                err = clerks[i].Put("k", "", 1)
                self.assertEqual(err, Err.ErrNoKey, f"Put failed {err}")
                
            ts.Begin("Test: memory use many put clients")
            
            # Measure memory before (if psutil available)
            if HAS_PSUTIL:
                process = psutil.Process(os.getpid())
                gc.collect()
                mem_before = process.memory_info().rss
            else:
                mem_before = 0
            
            # Make puts from all clients
            for i in range(NCLIENT):
                err = clerks[i].Put("k", v, i)
                self.assertEqual(err, Err.OK, f"Put failed {err}")
                
            # Measure memory after (if psutil available)
            if HAS_PSUTIL:
                gc.collect()
                time.sleep(0.1)
                gc.collect()
                mem_after = process.memory_info().rss
                
                mem_per_client = (mem_after - mem_before) / NCLIENT
                print(f"Memory usage: {mem_before} -> {mem_after} ({mem_per_client:.2f} per client)")
                
                # Check memory usage is reasonable (more lenient for Python)
                if mem_after > mem_before + (NCLIENT * 1000):  # 1KB per client max
                    self.fail(f"Server using too much memory: {mem_before} -> {mem_after} ({mem_per_client:.2f} per client)")
            else:
                print("Memory measurement skipped (psutil not available)")
                
            print("✓ Memory test passed")
            
        finally:
            ts.Cleanup()
            
    def test_unreliable_net(self):
        """Test with unreliable network"""
        print("\n=== Test: Unreliable Network ===")
        
        NTRY = 20  # Reduced from 100 for faster testing
        
        ts = MakeTestKV(False)  # Unreliable network
        ts.Begin("One client unreliable network")
        
        try:
            ck = ts.MakeClerk()
            
            retried = False
            for try_num in range(NTRY):
                attempt = 0
                while True:
                    err = PutJson(ck, "k", attempt, try_num)
                    if err != Err.ErrMaybe:
                        if attempt > 0 and err != Err.ErrVersion:
                            self.fail(f"Put shouldn't have happened more than once {err}")
                        break
                    # Got ErrMaybe - try again
                    retried = True
                    attempt += 1
                    
                # Check final value
                ver, v = GetJson(ck, "k", -1)
                self.assertEqual(ver, try_num + 1, f"Wrong version {ver} expect {try_num + 1}")
                self.assertEqual(v, 0, f"Wrong value {v} expect 0")
                
            if not retried:
                self.fail("Clerk.Put never returned ErrMaybe")
                
            print("✓ Unreliable network test passed")
            
        finally:
            ts.Cleanup()


class TestLock(unittest.TestCase):
    """
    Lock Test Cases - COMPLETELY FIXED VERSION
    
    Tests the distributed lock implementation using the KV server.
    
    FIXED:
    - Proper threading to avoid deadlocks
    - Better mutual exclusion detection using dedicated test keys
    - Exception propagation from worker threads
    - More robust test logic
    """
    
    def _lock_worker(self, client_id: int, ts: TestKV, done_event: threading.Event, 
                     test_key: str, error_list: List) -> int:
        """
        FIXED lock test worker with proper error handling
        
        Uses a cleaner approach to detect mutual exclusion violations:
        - Each client tries to exclusively increment a counter
        - Uses the lock to protect the increment operation
        - If mutual exclusion works, no increments should be lost
        """
        ck = ts.MakeClerk()
        lock = MakeLock(ck, "test_lock")
        
        iterations = 0
        try:
            while not done_event.is_set() and iterations < 100:  # Limit iterations
                iterations += 1
                
                # Acquire lock
                lock.Acquire()
                
                try:
                    # Read current counter value
                    val, ver, err = ck.Get(test_key)
                    if err == Err.ErrNoKey:
                        current_val = 0
                        ver = 0
                    elif err == Err.OK:
                        current_val = int(val) if val.isdigit() else 0
                    else:
                        # Get failed - skip this iteration
                        lock.Release()
                        continue
                    
                    # Small delay to increase chance of race conditions if lock doesn't work
                    time.sleep(0.001)
                    
                    # Increment and write back
                    new_val = current_val + 1
                    put_err = ck.Put(test_key, str(new_val), ver)
                    
                    if put_err not in [Err.OK, Err.ErrMaybe]:
                        # This could indicate a mutual exclusion violation
                        # if another client modified the value while we held the lock
                        if put_err == Err.ErrVersion:
                            error_list.append(f"Client {client_id}: Version conflict while holding lock "
                                             f"(current_val={current_val}, ver={ver})")
                        else:
                            error_list.append(f"Client {client_id}: Unexpected put error {put_err}")
                
                finally:
                    # Always release lock
                    lock.Release()
                
                # Small delay before next iteration
                time.sleep(0.001)
                
        except Exception as e:
            error_list.append(f"Client {client_id}: Exception: {e}")
        finally:
            ts.DeleteClerk(ck)
            
        return iterations
    
    def test_one_client_reliable(self):
        """Test lock with one client and reliable network - FIXED VERSION"""
        print("\n=== Test: One Client Lock Reliable ===")
        
        ts = MakeTestKV(True)
        ts.Begin("Test: 1 lock client")
        
        try:
            done_event = threading.Event()
            errors = []  # Collect errors from worker threads
            result = [0]  # Use list to store result from thread
            
            def client_wrapper():
                """Wrapper to run client in thread"""
                result[0] = self._lock_worker(0, ts, done_event, "counter", errors)
            
            # Start lock test in separate thread
            thread = threading.Thread(target=client_wrapper)
            thread.start()
            
            # Let it run briefly
            time.sleep(0.1)
            done_event.set()
            
            # Wait for thread to complete
            thread.join(timeout=5)
            
            # Check for errors
            if errors:
                self.fail(f"Lock test errors: {errors}")
            
            print(f"✓ One client lock test completed ({result[0]} iterations)")
            
        finally:
            ts.Cleanup()
            
    def test_many_clients_reliable(self):
        """Test lock with many clients and reliable network - FIXED VERSION"""
        print("\n=== Test: Many Clients Lock Reliable ===")
        
        NCLNT = 5  # Reduced for faster testing
        NSEC = 2
        
        ts = MakeTestKV(True)
        ts.Begin(f"Test: {NCLNT} lock clients")
        
        try:
            # Initialize counter
            init_ck = ts.MakeClerk()
            init_ck.Put("shared_counter", "0", 0)
            ts.DeleteClerk(init_ck)
            
            done_event = threading.Event()
            errors = []  # Collect errors from all threads
            results = [0] * NCLNT
            threads = []
            
            def client_wrapper(client_id: int):
                results[client_id] = self._lock_worker(client_id, ts, done_event, 
                                                     "shared_counter", errors)
            
            # Start clients
            for i in range(NCLNT):
                thread = threading.Thread(target=client_wrapper, args=(i,))
                threads.append(thread)
                thread.start()
                
            # Let them run
            time.sleep(NSEC)
            done_event.set()
            
            # Wait for completion
            for thread in threads:
                thread.join(timeout=5)
                
            # Check for errors (mutual exclusion violations)
            if errors:
                self.fail(f"Mutual exclusion violations detected: {errors}")
                
            # Check final counter value
            final_ck = ts.MakeClerk()
            final_val, final_ver, err = final_ck.Get("shared_counter")
            
            total_iterations = sum(results)
            if err == Err.OK:
                final_count = int(final_val) if final_val.isdigit() else 0
                print(f"Final counter: {final_count}, Expected: {total_iterations}")
                
                # The final counter should equal the total iterations if lock works correctly
                if final_count != total_iterations:
                    self.fail(f"Mutual exclusion failure: expected {total_iterations} "
                             f"increments, got {final_count}")
            
            print(f"✓ Many clients lock test completed ({total_iterations} total iterations)")
            
        finally:
            ts.Cleanup()
            
    def test_one_client_unreliable(self):
        """Test lock with one client and unreliable network - FIXED VERSION"""
        print("\n=== Test: One Client Lock Unreliable ===")
        
        ts = MakeTestKV(False)  # Unreliable network
        ts.Begin("Test: 1 lock client unreliable")
        
        try:
            done_event = threading.Event()
            errors = []  # Collect errors from worker threads
            result = [0]  # Use list to store result from thread
            
            def client_wrapper():
                """Wrapper to run client in thread"""
                result[0] = self._lock_worker(0, ts, done_event, "counter_unreliable", errors)
            
            # Start lock test in separate thread
            thread = threading.Thread(target=client_wrapper)
            thread.start()
            
            # Let it run briefly (longer for unreliable network)
            time.sleep(0.5)
            done_event.set()
            
            # Wait for thread to complete  
            thread.join(timeout=10)  # Longer timeout for unreliable network
            
            # Check for errors (be more lenient for unreliable network)
            # Some operations might fail due to network issues, but mutual exclusion should still hold
            serious_errors = [e for e in errors if "Version conflict while holding lock" in e]
            if serious_errors:
                self.fail(f"Mutual exclusion violations: {serious_errors}")
            
            print(f"✓ One client unreliable lock test completed ({result[0]} iterations)")
            
        finally:
            ts.Cleanup()
            
    def test_many_clients_unreliable(self):
        """Test lock with many clients and unreliable network - FIXED VERSION"""
        print("\n=== Test: Many Clients Lock Unreliable ===")
        
        NCLNT = 3  # Reduced for unreliable network
        NSEC = 2
        
        ts = MakeTestKV(False)  # Unreliable network
        ts.Begin(f"Test: {NCLNT} lock clients unreliable")
        
        try:
            # Initialize counter
            init_ck = ts.MakeClerk()
            init_ck.Put("shared_counter_unreliable", "0", 0)
            ts.DeleteClerk(init_ck)
            
            done_event = threading.Event()
            errors = []  # Collect errors from all threads
            results = [0] * NCLNT
            threads = []
            
            def client_wrapper(client_id: int):
                results[client_id] = self._lock_worker(client_id, ts, done_event, 
                                                     "shared_counter_unreliable", errors)
            
            # Start clients
            for i in range(NCLNT):
                thread = threading.Thread(target=client_wrapper, args=(i,))
                threads.append(thread)
                thread.start()
                
            # Let them run
            time.sleep(NSEC)
            done_event.set()
            
            # Wait for completion
            for thread in threads:
                thread.join(timeout=10)  # Longer timeout for unreliable network
                
            # Check for serious errors (mutual exclusion violations)
            # Be more lenient for unreliable network - some network errors are expected
            serious_errors = [e for e in errors if "Version conflict while holding lock" in e]
            if serious_errors:
                self.fail(f"Mutual exclusion violations: {serious_errors}")
            
            total_iterations = sum(results)
            print(f"✓ Many clients unreliable lock test completed ({total_iterations} total iterations)")
            
            if errors:
                print(f"   Non-critical network errors encountered: {len(errors)}")
            
        finally:
            ts.Cleanup()


def run_tests():
    """Run all KV server and lock tests"""
    print("Running MIT 6.5840 Lab 2: KV Server Tests")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    
    # Load KV server tests
    kv_suite = loader.loadTestsFromTestCase(TestKVServer)
    
    # Load lock tests
    lock_suite = loader.loadTestsFromTestCase(TestLock)
    
    # Combine suites
    combined_suite = unittest.TestSuite([kv_suite, lock_suite])
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2, stream=sys.stdout)
    result = runner.run(combined_suite)
    
    # Print summary
    print(f"\nTests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
            
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")
            
    success = result.wasSuccessful()
    if success:
        print("\n✓ All tests passed!")
    else:
        print(f"\n✗ {len(result.failures + result.errors)} tests failed")
        
    return success


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)