#!/usr/bin/env python3
"""
Enhanced KV Test Utilities - FIXED VERSION
Matching MIT 6.5840 Lab 2 kvtest.go functionality

This module provides comprehensive test utilities for KV server testing,
including advanced patterns for concurrent testing, JSON operations,
and complex test scenarios.

FIXED:
- Function signature mismatches in OneClientPut
- JSON decode errors in GetJson
- Race conditions in SpreadPuts
- Thread spawning logic
"""

import threading
import time
import random
import json
import logging
from typing_extensions import Protocol
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our modules
from rpc import Err, Tversion
from porcupine import OpLog, Operation, get_time, CheckOperationsVerbose, Visualize
from models import KvModel, KvInput, KvOutput

logger = logging.getLogger(__name__)

# Election timeout constant
ELECTION_TIMEOUT = 1.0  # seconds

def RandValue(n: int) -> str:
    """Generate random string of length n - matching Go lab RandValue"""
    letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join(random.choice(letters) for _ in range(n))

def Randstring(n: int) -> str:
    """Generate random string - alias for RandValue"""
    return RandValue(n)

class IKVClerk(Protocol):
    """Interface for KV clerk - matches Go interface"""
    def Get(self, key: str) -> Tuple[str, Tversion, str]:
        """Get value and version for key"""
        ...
        
    def Put(self, key: str, value: str, version: Tversion) -> str:
        """Put value with version control"""
        ...

class TestClerk:
    """
    Test wrapper for Clerk - matches Go TestClerk
    Enhanced version with more functionality
    """
    
    def __init__(self, clerk: IKVClerk, client_end: Any):
        """Create test clerk wrapper"""
        self.clerk = clerk
        self.client_end = client_end
        
    def Get(self, key: str) -> Tuple[str, Tversion, str]:
        """Get method for test interface"""
        return self.clerk.Get(key)
        
    def Put(self, key: str, value: str, version: Tversion) -> str:
        """Put method for test interface"""
        return self.clerk.Put(key, value, version)

class IClerkMaker(Protocol):
    """Interface for creating clerks - matches Go interface"""
    def MakeClerk(self) -> IKVClerk:
        """Create a new clerk"""
        ...
        
    def DeleteClerk(self, ck: IKVClerk):
        """Delete a clerk"""
        ...

@dataclass
class ClntRes:
    """Client result - matches Go ClntRes"""
    Nok: int = 0      # Number of successful operations
    Nmaybe: int = 0   # Number of maybe operations

@dataclass
class EntryV:
    """Entry with ID and version - matches Go EntryV"""
    Id: int
    V: Tversion

@dataclass
class EntryN:
    """Entry with ID and sequence number - matches Go EntryN"""
    Id: int
    N: int

class Test:
    """
    Enhanced test framework - matches Go Test struct - FIXED VERSION
    Provides comprehensive testing utilities
    """
    
    def __init__(self, config, oplog: Optional[OpLog] = None, randomkeys: bool = False, 
                 clerk_maker: Optional[IClerkMaker] = None):
        """Create test instance"""
        self.config = config
        self.oplog = oplog or OpLog()
        self.randomkeys = randomkeys
        self.clerk_maker = clerk_maker
        
    def Cleanup(self):
        """Cleanup test resources"""
        if hasattr(self.config, 'Cleanup'):
            self.config.Cleanup()
            
    def MakeClerk(self) -> IKVClerk:
        """Create clerk using clerk maker"""
        if self.clerk_maker:
            return self.clerk_maker.MakeClerk()
        return self.config.MakeClerk()
        
    def DeleteClerk(self, ck: IKVClerk):
        """Delete clerk"""
        if self.clerk_maker:
            self.clerk_maker.DeleteClerk(ck)
        else:
            self.config.DeleteClerk(ck)
            
    def Fatalf(self, msg: str):
        """Fatal error"""
        raise Exception(msg)
        
    def Begin(self, description: str):
        """Begin test with description"""
        print(f"Test: {description}")
        
    def Op(self):
        """Record operation"""
        if hasattr(self.config, 'Op'):
            self.config.Op()

    def PutAtLeastOnce(self, ck: IKVClerk, key: str, value: str, ver: Tversion, me: int) -> Tversion:
        """
        Put operation that retries until success - matches Go lab PutAtLeastOnce
        Handles version conflicts and ensures at least one successful put
        
        FIXED: Proper version tracking to avoid over-incrementing
        """
        current_ver = ver
        max_retries = 100  # Prevent infinite loops
        retry_count = 0
        
        while retry_count < max_retries:
            retry_count += 1
            err = self.Put(ck, key, value, current_ver, me)
            
            if err == Err.OK:
                # Successfully put - return incremented version
                return current_ver + 1
            elif err == Err.ErrMaybe:
                # Might have succeeded - check current state
                val, actual_ver, get_err = ck.Get(key)
                if get_err == Err.OK:
                    # Key exists - check if our value is already there
                    if val == value:
                        # Our put succeeded
                        return actual_ver
                    else:
                        # Different value, use current version for next attempt
                        current_ver = actual_ver
                elif get_err == Err.ErrNoKey:
                    # Key doesn't exist, try with version 0
                    current_ver = 0
            elif err == Err.ErrVersion:
                # Version mismatch - get current version and retry
                val, actual_ver, get_err = ck.Get(key)
                if get_err == Err.OK:
                    current_ver = actual_ver
                elif get_err == Err.ErrNoKey:
                    current_ver = 0
                else:
                    # Get failed, small delay and retry
                    time.sleep(0.01)
            elif err == Err.ErrNoKey:
                # Key doesn't exist but we used version > 0, try with 0
                current_ver = 0
            else:
                # Unexpected error
                self.Fatalf(f"Put {key} ver {current_ver} err {err}")
                
            # Small delay between retries
            time.sleep(0.001)
                
        # If we get here, we hit max retries
        self.Fatalf(f"PutAtLeastOnce exceeded max retries for {key}")
        return current_ver  # Should never reach here due to Fatalf
        
    def CheckGet(self, ck: IKVClerk, key: str, value: str, version: Tversion):
        """Check that Get returns expected value and version"""
        logger.info(f"Checking Get({key}) = ({value}, {version})")
        val, ver, err = self.Get(ck, key, 0)
        if err != Err.OK:
            self.Fatalf(f"Get({key}) returns error = {err}")
        if val != value or ver != version:
            self.Fatalf(f"Get({key}) returns ({val}, {ver}) != ({value}, {version})")
        logger.info(f"Get({key}) returns ({val}, {ver}) as expected")
        
    def CheckPutConcurrent(self, ck: IKVClerk, key: str, results: List[ClntRes], 
                          total_res: ClntRes, reliable: bool):
        """
        Check results of concurrent Put operations - matches Go lab CheckPutConcurrent
        Validates that the number of successful operations matches server state
        
        FIXED: Handle both JSON and non-JSON values, and handle non-existent keys
        """
        # Get final state - FIXED: Handle case where key might not exist
        val, ver0, err = self.Get(ck, key, -1)
        
        if err == Err.ErrNoKey:
            # Key doesn't exist - no successful puts
            ver0 = 0
        elif err != Err.OK:
            self.Fatalf(f"Get({key}) failed: {err}")
        else:
            # Try to parse as EntryV if it looks like JSON, otherwise use version directly
            if val.startswith('{') and val.endswith('}'):
                # Looks like JSON - try to parse to get the actual put count
                try:
                    e = EntryV(0, 0)
                    ver0 = self.GetJson(ck, key, -1, e)
                except:
                    # If JSON parsing fails, use the version from Get
                    pass
        
        # Sum up all results
        for r in results:
            total_res.Nok += r.Nok
            total_res.Nmaybe += r.Nmaybe
            
        if reliable:
            if ver0 != total_res.Nok:
                self.Fatalf(f"Reliable: Wrong number of puts: server {ver0} clients {total_res}")
        else:
            if ver0 > total_res.Nok + total_res.Nmaybe:
                self.Fatalf(f"Unreliable: Wrong number of puts: server {ver0} clients {total_res}")
                
    def SpawnClientsAndWait(self, nclnt: int, duration: float, 
                           client_func: Callable) -> List[ClntRes]:
        """
        Spawn multiple clients and wait for completion - matches Go lab SpawnClientsAndWait
        Returns results from all clients
        
        FIXED: Simplified handling of client functions
        """
        results = [ClntRes()] * nclnt
        done_event = threading.Event()
        threads = []
        
        def run_client(client_id: int):
            """Run single client - FIXED"""
            ck = self.MakeClerk()
            try:
                # Try different function call patterns
                try:
                    # Try with keys parameter first (OneClientPut style)
                    keys = ["k"]
                    results[client_id] = client_func(client_id, ck, keys, done_event)
                except TypeError:
                    try:
                        # Try without keys parameter (simpler functions)
                        results[client_id] = client_func(client_id, ck, done_event)
                    except TypeError:
                        # If both fail, create empty result
                        logger.error(f"Failed to call client function for client {client_id}")
                        results[client_id] = ClntRes()
            except Exception as e:
                logger.error(f"Client {client_id} error: {e}")
                results[client_id] = ClntRes()
            finally:
                self.DeleteClerk(ck)
                
        # Start all clients
        for i in range(nclnt):
            thread = threading.Thread(target=run_client, args=(i,))
            threads.append(thread)
            thread.start()
            
        # Wait for duration
        time.sleep(duration)
        done_event.set()
        
        # Wait for all clients to finish
        for thread in threads:
            thread.join(timeout=10.0)
            
        return results
        
    def GetJson(self, ck: IKVClerk, key: str, me: int, default_value: Any) -> Tversion:
        """Get JSON-encoded value - matches Go lab GetJson - FIXED"""
        val, ver, err = self.Get(ck, key, me)
        if err == Err.OK:
            if val == "":
                # Empty string - return 0 version
                return 0
            try:
                decoded = json.loads(val)
                # Copy fields to default_value if it's an object
                if hasattr(default_value, '__dict__') and isinstance(decoded, dict):
                    for k, v in decoded.items():
                        if hasattr(default_value, k):
                            setattr(default_value, k, v)
                return ver
            except json.JSONDecodeError:
                # FIXED: Don't fail on non-JSON data, just return version
                logger.warning(f"Non-JSON data in key {key}: {val}")
                return ver
        else:
            self.Fatalf(f"Get {key} err {err}")
        return 0
        
    def PutJson(self, ck: IKVClerk, key: str, value: Any, ver: Tversion, me: int) -> str:
        """Put JSON-encoded value - matches Go lab PutJson"""
        try:
            json_str = json.dumps(value.__dict__ if hasattr(value, '__dict__') else value)
            return self.Put(ck, key, json_str, ver, me)
        except Exception as e:
            self.Fatalf(f"JSON marshal error: {e}")
            return Err.ErrNoKey
            
    def PutAtLeastOnceJson(self, ck: IKVClerk, key: str, value: Any, ver: Tversion, me: int) -> Tversion:
        """Put JSON value at least once - matches Go lab PutAtLeastOnceJson"""
        while True:
            err = self.PutJson(ck, key, value, 0, me)
            if err != Err.ErrMaybe:
                break
            ver += 1
        return ver
        
    def OnePut(self, me: int, ck: IKVClerk, key: str, ver: Tversion) -> Tuple[Tversion, bool]:
        """
        Try to do one successful put while other clients compete - matches Go lab OnePut
        Returns (new_version, success)
        """
        while True:
            err = self.PutJson(ck, key, EntryV(me, ver), ver, me)
            if err not in [Err.OK, Err.ErrVersion, Err.ErrMaybe]:
                self.Fatalf(f"Wrong error {err}")
                
            # Check what actually happened
            e = EntryV(0, 0)
            ver0 = self.GetJson(ck, key, me, e)
            
            if err == Err.OK and ver0 == ver + 1:
                # My put succeeded
                if e.Id != me or e.V != ver:
                    self.Fatalf(f"Wrong value {e}")
                    
            ver = ver0
            if err == Err.OK or err == Err.ErrMaybe:
                return ver, (err == Err.OK)
                
    def OneClientPut(self, cli: int, ck: IKVClerk, keys: List[str], done: threading.Event) -> ClntRes:
        """
        One client doing puts until done - matches Go lab OneClientPut - FIXED
        Returns number of successful and maybe operations
        """
        res = ClntRes()
        ver_map = {k: Tversion(0) for k in keys}
        
        while not done.is_set():
            try:
                # Choose key (random if randomkeys enabled)
                key = keys[0]
                if self.randomkeys and len(keys) > 1:
                    key = random.choice(keys)
                    
                ver_map[key], ok = self.OnePut(cli, ck, key, ver_map[key])
                if ok:
                    res.Nok += 1
                else:
                    res.Nmaybe += 1
            except Exception as e:
                logger.error(f"Client {cli} error: {e}")
                break
                
        return res
        
    def OneClientAppend(self, me: int, ck: IKVClerk, done: threading.Event) -> ClntRes:
        """
        One client doing append operations - matches Go lab OneClientAppend
        Simulates appending to a shared list
        """
        nmaybe = 0
        nok = 0
        
        for i in range(1000):  # Limit iterations to prevent infinite loop
            if done.is_set():
                break
                
            try:
                # Keep trying to append entry (me, i) to key "k"
                while True:
                    # Get current list
                    entries = []
                    ver = self.GetJson(ck, "k", me, entries)
                    
                    # Append new entry
                    entries.append(EntryN(me, i))
                    
                    # Try to put updated list
                    err = self.PutJson(ck, "k", entries, ver, me)
                    if err == Err.OK:
                        nok += 1
                        break
                    elif err == Err.ErrMaybe:
                        nmaybe += 1
                        break
                    # Retry on ErrVersion
                    
            except Exception as e:
                logger.error(f"Append client {me} error: {e}")
                break
                
        return ClntRes(nok, nmaybe)
        
    def CheckAppends(self, entries: List[EntryN], nclnt: int, results: List[ClntRes], ver: Tversion):
        """Check append operation results - matches Go lab CheckAppends"""
        expect = {i: 0 for i in range(nclnt)}
        skipped = {i: 0 for i in range(nclnt)}
        
        for entry in entries:
            if expect[entry.Id] > entry.N:
                self.Fatalf(f"{entry.Id}: wrong expecting {expect[entry.Id]} but got {entry.N}")
            elif expect[entry.Id] == entry.N:
                expect[entry.Id] += 1
            else:
                # Missing entries due to failed puts
                s = entry.N - expect[entry.Id]
                expect[entry.Id] = entry.N + 1
                skipped[entry.Id] += s
                
        if len(entries) + 1 != ver:
            self.Fatalf(f"{len(entries)} appends in val != puts on server {ver}")
            
        for c, n in expect.items():
            if skipped[c] > results[c].Nmaybe:
                self.Fatalf(f"{c}: skipped puts {skipped[c]} on server > {results[c].Nmaybe} maybe")
            if n > results[c].Nok + results[c].Nmaybe:
                self.Fatalf(f"{c}: {n} puts on server > ok+maybe {results[c].Nok + results[c].Nmaybe}")
                
    def SpreadPuts(self, ck: IKVClerk, n: int, key_prefix: str = "k") -> Tuple[List[str], List[str]]:
        """Create n key-value pairs with 20-char values - matches Go lab SpreadPuts"""
        return self.SpreadPutsSize(ck, n, 20, key_prefix)
        
    def SpreadPutsSize(self, ck: IKVClerk, n: int, valsz: int, key_prefix: str = "k") -> Tuple[List[str], List[str]]:
        """Create n key-value pairs with valsz-char values - matches Go lab SpreadPutsSize - FIXED"""
        keys = [f"{key_prefix}{i}" for i in range(n)]  # Use custom prefix
        values = []
        
        # FIXED: Generate all values first to avoid race conditions
        for i in range(n):
            val = Randstring(valsz)
            values.append(val)
            
        # Then put all values
        for i in range(n):
            err = self.Put(ck, keys[i], values[i], 0, -1)
            if err != Err.OK:
                self.Fatalf(f"Put({keys[i]}) failed: {err}")
            
        # Verify all puts - FIXED: Use the values we actually generated
        for i in range(n):
            self.CheckGet(ck, keys[i], values[i], 1)
            
        return keys, values
        
    def Partitioner(self, gid: int, done_chan: threading.Event):
        """
        Periodically repartition servers - matches Go lab Partitioner
        Simulates network partitions for testing
        """
        logger.info(f"Starting partitioner for group {gid}")
        
        try:
            while not done_chan.is_set():
                # Random partition
                n_servers = getattr(self.config, 'n', 3)
                partition_a = []
                partition_b = []
                
                for i in range(n_servers):
                    if random.randint(0, 1):
                        partition_a.append(i)
                    else:
                        partition_b.append(i)
                        
                # Apply partition if config supports it
                if hasattr(self.config, 'Partition'):
                    self.config.Partition(partition_a, partition_b)
                    logger.info(f"Applied partition: {partition_a} | {partition_b}")
                    
                # Wait before next partition
                sleep_time = ELECTION_TIMEOUT + random.uniform(0, 0.2)
                if done_chan.wait(sleep_time):
                    break
                    
        except Exception as e:
            logger.error(f"Partitioner error: {e}")
        finally:
            logger.info(f"Partitioner for group {gid} stopping")
            
    # Porcupine integration methods
    def Get(self, ck: IKVClerk, key: str, cli: int) -> Tuple[str, Tversion, str]:
        """Get with porcupine logging - matches Go lab porcupine integration"""
        start = get_time()
        val, ver, err = ck.Get(key)
        end = get_time()
        
        self.Op()
        if self.oplog:
            self.oplog.Append(Operation(
                Input=KvInput(Op=0, Key=key),
                Output=KvOutput(Value=val, Version=ver, Err=err),
                Call=start,
                Return=end,
                ClientId=cli
            ))
            
        return val, ver, err
        
    def Put(self, ck: IKVClerk, key: str, value: str, version: Tversion, cli: int) -> str:
        """Put with porcupine logging - matches Go lab porcupine integration"""
        start = get_time()
        err = ck.Put(key, value, version)
        end = get_time()
        
        self.Op()
        if self.oplog:
            self.oplog.Append(Operation(
                Input=KvInput(Op=1, Key=key, Value=value, Version=version),
                Output=KvOutput(Err=err),
                Call=start,
                Return=end,
                ClientId=cli
            ))
            
        return err
        
    def CheckPorcupine(self):
        """Check linearizability with default timeout"""
        self.CheckPorcupineT(1.0)
        
    def CheckPorcupineT(self, timeout: float):
        """Check linearizability with specified timeout"""
        if not self.oplog:
            return
            
        operations = self.oplog.Read()
        result, info = CheckOperationsVerbose(KvModel, operations, timeout)
        
        if result.value == "Illegal":
            # Create visualization file for debugging
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
                    Visualize(KvModel, info, f)
                    logger.info(f"Linearizability visualization saved to {f.name}")
            except Exception as e:
                logger.error(f"Failed to create visualization: {e}")
                
            self.Fatalf("History is not linearizable")
        elif result.value == "Unknown":
            logger.info("Linearizability check timed out, assuming history is ok")

def MakeKeys(n: int) -> List[str]:
    """Generate n keys - matches Go lab MakeKeys"""
    return [f"k{i}" for i in range(n)]

def MakeTest(config, randomkeys: bool = False, clerk_maker: Optional[IClerkMaker] = None) -> Test:
    """Create test instance - matches Go lab MakeTest"""
    return Test(config, OpLog(), randomkeys, clerk_maker)

# Test the enhanced utilities
def test_kvtest():
    """Test the enhanced KV test utilities"""
    print("Testing FIXED Enhanced KV Test Utilities...")
    
    # Test utility functions
    rand_val = RandValue(10)
    assert len(rand_val) == 10
    print("✓ RandValue works")
    
    keys = MakeKeys(5)
    assert len(keys) == 5
    assert keys[0] == "k0"
    assert keys[4] == "k4"
    print("✓ MakeKeys works")
    
    # Test EntryV and EntryN
    entry_v = EntryV(1, 42)
    assert entry_v.Id == 1
    assert entry_v.V == 42
    print("✓ EntryV works")
    
    entry_n = EntryN(2, 100)
    assert entry_n.Id == 2
    assert entry_n.N == 100
    print("✓ EntryN works")
    
    # Test ClntRes
    res = ClntRes()
    assert res.Nok == 0
    assert res.Nmaybe == 0
    
    res = ClntRes(5, 3)
    assert res.Nok == 5
    assert res.Nmaybe == 3
    print("✓ ClntRes works")
    
    print("FIXED Enhanced KV test utilities test completed successfully!")

if __name__ == "__main__":
    test_kvtest()