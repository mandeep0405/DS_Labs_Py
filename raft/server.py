#!/usr/bin/env python3
"""
Raft Server Implementation - server.py
FIXED to properly use labrpc network simulation

This represents the service/state machine layer that sits on top of Raft.
Now properly integrated with the enhanced_labrpc network simulation.
"""

import threading
import time
import pickle
import queue
import random
from typing import Dict, Any, Optional, List
import logging

# Import enhanced modules
from labrpc import MakeNetwork, MakeServer, MakeService, ClientEnd, Network, Server
from persister import MakePersister, Persister
from raft import Make, Raft, ApplyMsg

logger = logging.getLogger(__name__)

# Constants matching Go lab
SNAPSHOT_INTERVAL = 10

class RaftServer:
    """
    Raft Server - exactly matching rfsrv struct from server.go
    NOW PROPERLY INTEGRATED WITH LABRPC
    
    This represents the application/service layer that uses Raft for consensus.
    It receives applied entries from Raft and maintains the state machine.
    """
    
    def __init__(self, test_config, me: int, ends: List[ClientEnd], persister: Persister, snapshot: bool = False):
        """
        Create new raft server - matching newRfsrv() from server.go
        FIXED to properly use ClientEnd objects
        
        Args:
            test_config: Test configuration (equivalent to *Test in Go)
            me: Server ID 
            ends: List of ClientEnd objects for RPC communication
            persister: Enhanced persister for durable storage
            snapshot: Whether to enable snapshots
        """
        self.test_config = test_config
        self.me = me
        self.apply_err = ""  # Error from apply channel readers
        self.last_applied = 0
        self.persister = persister
        
        self.mu = threading.Lock()
        self.logs: Dict[int, Any] = {0: None}  # Copy of each server's committed entries
        
        # Create apply channel
        self.apply_ch = queue.Queue()
        
        # Create Raft instance with proper ClientEnd objects
        self.raft = Make(ends, me, persister, self.apply_ch)
        
        # Create labrpc server to handle incoming RPCs
        self.rpc_server = MakeServer()
        
        # Create RPC service that wraps our Raft instance
        raft_service = RaftRPCService(self.raft)
        service = MakeService("Raft", raft_service)
        self.rpc_server.AddService(service)
        
        # Handle snapshots if enabled
        if snapshot:
            snapshot_data = persister.ReadSnapshot()
            if snapshot_data:
                # Process existing snapshot
                err = self.ingest_snap(snapshot_data, -1)
                if err:
                    raise Exception(f"Failed to ingest snapshot: {err}")
                    
            # Start snapshot-enabled applier
            threading.Thread(target=self.applier_snap, daemon=True).start()
        else:
            # Start regular applier  
            threading.Thread(target=self.applier, daemon=True).start()
            
    def get_rpc_server(self) -> Server:
        """Get the labrpc server for this Raft instance"""
        return self.rpc_server
            
    def Kill(self):
        """Kill the server - matching Kill() from server.go"""
        with self.mu:
            if self.raft:
                self.raft.Kill()
            self.raft = None
            
        if self.persister:
            # Mimic KV server that saves persistent state in case it restarts
            raft_log = self.persister.ReadRaftState()
            snapshot = self.persister.ReadSnapshot()
            self.persister.Save(raft_log, snapshot)
            
    def GetState(self) -> tuple:
        """Get state from underlying Raft - matching GetState() from server.go"""
        with self.mu:
            if self.raft is None:
                return 0, False
            return self.raft.GetState()
            
    def RaftInstance(self) -> Optional[Raft]:
        """Get Raft instance - matching Raft() from server.go"""
        with self.mu:
            return self.raft
            
    def Logs(self, index: int) -> tuple:
        """Get log entry at index - matching Logs() from server.go"""
        with self.mu:
            exists = index in self.logs
            value = self.logs.get(index)
            return value, exists
            
    def applier(self):
        """
        Apply committed entries - matching applier() from server.go
        
        Reads messages from apply channel and validates they match expected log contents
        """
        while True:
            try:
                msg = self.apply_ch.get(timeout=0.1)
                
                if not msg.CommandValid:
                    # Ignore other types of ApplyMsg
                    continue
                    
                # Check logs with test framework - matching server.go logic
                err_msg, prev_ok = self.test_config.check_logs(self.me, msg)
                
                if msg.CommandIndex > 1 and not prev_ok:
                    err_msg = f"server {self.me} apply out of order {msg.CommandIndex}"
                    
                if err_msg:
                    logger.error(f"Apply error: {err_msg}")
                    self.apply_err = err_msg
                    # Keep reading after error so Raft doesn't block
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Applier error: {e}")
                break
                
    def applier_snap(self):
        """
        Apply entries with snapshot support - matching applierSnap() from server.go
        
        Handles both regular entries and snapshots, creating periodic snapshots
        """
        if self.raft is None:
            return
            
        while True:
            try:
                msg = self.apply_ch.get(timeout=0.1)
                err_msg = ""
                
                if msg.SnapshotValid:
                    # Handle snapshot
                    err_msg = self.ingest_snap(msg.Snapshot, msg.SnapshotIndex)
                    
                elif msg.CommandValid:
                    # Handle regular command
                    if msg.CommandIndex != self.last_applied + 1:
                        err_msg = f"server {self.me} apply out of order, expected index {self.last_applied + 1}, got {msg.CommandIndex}"
                        
                    if not err_msg:
                        err_msg, prev_ok = self.test_config.check_logs(self.me, msg)
                        if msg.CommandIndex > 1 and not prev_ok:
                            err_msg = f"server {self.me} apply out of order {msg.CommandIndex}"
                            
                    self.last_applied = msg.CommandIndex
                    
                    # Create snapshot periodically - matching server.go logic
                    if (msg.CommandIndex + 1) % SNAPSHOT_INTERVAL == 0:
                        self._create_snapshot(msg.CommandIndex)
                        
                else:
                    # Ignore other types of ApplyMsg
                    continue
                    
                if err_msg:
                    logger.error(f"Apply error: {err_msg}")
                    self.apply_err = err_msg
                    # Keep reading after error so Raft doesn't block
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Applier snap error: {e}")
                break
                
    def _create_snapshot(self, index: int):
        """Create snapshot at given index - matching server.go snapshot logic"""
        # Encode snapshot data - matching Go labgob encoding
        snapshot_data = {
            'lastIncludedIndex': index,
            'logs': []
        }
        
        # Include all logs up to index
        for j in range(index + 1):
            if j in self.logs:
                snapshot_data['logs'].append(self.logs[j])
            else:
                snapshot_data['logs'].append(None)
                
        # Serialize snapshot
        encoded_snapshot = pickle.dumps(snapshot_data)
        
        # Tell Raft to create snapshot
        if self.raft:
            logger.info(f"Server {self.me}: Creating snapshot at index {index}")
            self.raft.Snapshot(index, encoded_snapshot)
            
    def ingest_snap(self, snapshot: bytes, index: int) -> str:
        """
        Ingest snapshot - matching ingestSnap() from server.go
        
        Returns empty string on success, error message on failure
        """
        with self.mu:
            if not snapshot:
                logger.error("Nil snapshot")
                return "nil snapshot"
                
            try:
                # Decode snapshot - matching Go labgob decoding
                snapshot_data = pickle.loads(snapshot)
                last_included_index = snapshot_data['lastIncludedIndex']
                xlog = snapshot_data['logs']
                
            except Exception as e:
                logger.error("Snapshot decode error")
                return "snapshot decode error"
                
            if index != -1 and index != last_included_index:
                err = f"server {self.me} snapshot doesn't match SnapshotIndex"
                return err
                
            # Restore logs from snapshot
            self.logs = {}
            for j in range(len(xlog)):
                if xlog[j] is not None:
                    self.logs[j] = xlog[j]
                    
            self.last_applied = last_included_index
            return ""


class RaftRPCService:
    """
    RPC Service wrapper for Raft instance
    
    This class provides the RPC interface that the labrpc network expects.
    It wraps the Raft instance and handles incoming RPCs.
    """
    
    def __init__(self, raft_instance: Raft):
        """Create RPC service wrapper"""
        self.rf = raft_instance
        
    def RequestVote(self, args, reply):
        """Handle incoming RequestVote RPC"""
        if self.rf is None:
            return
            
        result = self.rf.RequestVote(args)
        
        # Copy result to reply object (labrpc expects modification in place)
        reply.Term = result.Term
        reply.VoteGranted = result.VoteGranted
        
    def AppendEntries(self, args, reply):
        """Handle incoming AppendEntries RPC"""
        if self.rf is None:
            return
            
        result = self.rf.AppendEntries(args)
        
        # Copy result to reply object (labrpc expects modification in place)
        reply.Term = result.Term
        reply.Success = result.Success
        reply.ConflictIndex = result.ConflictIndex
        reply.ConflictTerm = result.ConflictTerm
        
    def InstallSnapshot(self, args, reply):
        """Handle incoming InstallSnapshot RPC"""
        if self.rf is None:
            return
            
        result = self.rf.InstallSnapshot(args)
        
        # Copy result to reply object (labrpc expects modification in place)
        reply.Term = result.Term


class TestConfig:
    """
    Enhanced test configuration using labrpc network simulation
    FIXED to properly use labrpc infrastructure
    """
    
    def __init__(self, n: int, reliable: bool = True, snapshot: bool = False):
        self.n = n  # Number of servers
        self.reliable = reliable
        self.snapshot = snapshot
        
        # Enhanced network simulation - matching Go lab
        self.net = MakeNetwork()
        self.net.Reliable(reliable)
        self.net.LongDelays(True)  # Match Go config.SetLongDelays(true)
        
        # Server infrastructure
        self.servers: List[RaftServer] = []
        self.persisters: List[Persister] = []
        self.ends: List[List[ClientEnd]] = []  # Client endpoints matrix
        
        self.mu = threading.Lock()
        self.finished = False
        self.max_index = 0
        self.connected = [True] * n  # Track connected servers
        
        # Create cluster
        self._create_cluster()
        
    def _create_cluster(self):
        """Create cluster with enhanced network simulation"""
        # Create persisters using enhanced persister
        for i in range(self.n):
            self.persisters.append(MakePersister())
            
        # Create client endpoints matrix (like Go lab)
        for i in range(self.n):
            row = []
            for j in range(self.n):
                end = self.net.MakeEnd(f"end_{i}_{j}")
                row.append(end)
            self.ends.append(row)
            
        # Create servers and Raft instances
        for i in range(self.n):
            self._make_server(i)
            
        # Connect all endpoints
        self._connect_all()
        
    def _make_server(self, server_id: int):
        """Create single server with Raft instance - matching Go lab mksrv"""
        # Create RaftServer with proper ClientEnd objects
        server = RaftServer(self, server_id, self.ends[server_id], self.persisters[server_id], self.snapshot)
        self.servers.append(server)
        
        # Add server to network with its RPC server
        self.net.AddServer(server_id, server.get_rpc_server())
        
    def _connect_all(self):
        """Connect all endpoints to servers"""
        for i in range(self.n):
            for j in range(self.n):
                self.net.Connect(f"end_{i}_{j}", j)
                self.net.Enable(f"end_{i}_{j}", True)
            
    def check_logs(self, server_id: int, msg: ApplyMsg) -> tuple:
        """
        Check log consistency - matching checkLogs() from test.go
        
        Returns (error_message, prev_ok)
        """
        with self.mu:
            err_msg = ""
            value = msg.Command
            me = self.servers[server_id]
            
            # Check if any other server has different value at this index
            for j, server in enumerate(self.servers):
                old_value, old_ok = server.Logs(msg.CommandIndex)
                if old_ok and old_value != value:
                    err_msg = f"commit index={msg.CommandIndex} server={server_id} {msg.Command} != server={j} {old_value}"
                    break
                    
            # Check if previous entry exists
            _, prev_ok = me.Logs(msg.CommandIndex - 1)
            
            # Update logs
            me.logs[msg.CommandIndex] = value
            if msg.CommandIndex > self.max_index:
                self.max_index = msg.CommandIndex
                
            return err_msg, prev_ok
            
        
    def check_one_leader(self) -> Optional[int]:
        """Check that exactly one leader exists - matching Go lab checkOneLeader()"""
        for iters in range(20):  # Increased iterations to wait longer
            ms = 450 + (random.randint(0, 99))
            time.sleep(ms / 1000.0)
            
            leaders = {}
            leader_count_by_term = {}
            
            for i in range(self.n):
                if self._is_connected(i):
                    try:
                        term, is_leader = self.servers[i].GetState()
                        if is_leader:
                            if term not in leaders:
                                leaders[term] = []
                                leader_count_by_term[term] = 0
                            leaders[term].append(i)
                            leader_count_by_term[term] += 1
                    except:
                        # Server might be in transition
                        continue
                        
            # Find the highest term with exactly one leader
            highest_valid_term = -1
            for term, count in leader_count_by_term.items():
                if count == 1 and term > highest_valid_term:
                    highest_valid_term = term
                    
            # If we found a term with exactly one leader, return it
            if highest_valid_term >= 0:
                return leaders[highest_valid_term][0]
                
            # Log split brain for debugging
            for term, term_leaders in leaders.items():
                if len(term_leaders) > 1:
                    logger.warning(f"Split brain in term {term}: {term_leaders}")
                    
        raise Exception("Expected one leader, got none")
        
    def check_terms(self) -> int:
        """Check all servers agree on term - matching Go lab checkTerms()"""
        term = -1
        for i in range(self.n):
            if self._is_connected(i):
                xterm, _ = self.servers[i].GetState()
                if term == -1:
                    term = xterm
                elif term != xterm:
                    raise Exception(f"Servers disagree on term: {term} vs {xterm}")
        return term
        
    def check_no_leader(self):
        """Check no connected server thinks it's leader - matching Go lab checkNoLeader()"""
        for i in range(self.n):
            if self._is_connected(i):
                _, is_leader = self.servers[i].GetState()
                if is_leader:
                    raise Exception(f"Unexpected leader: {i}")
                    
    def check_no_agreement(self, index: int):
        """Check no unexpected agreement at index - matching Go lab checkNoAgreement()"""
        n, _ = self.n_committed(index)
        if n > 0:
            raise Exception(f"{n} committed but no majority")
            
    def n_committed(self, index: int) -> tuple:
        """Count committed entries at index - matching Go lab nCommitted()"""
        count = 0
        cmd = None
        
        for server in self.servers:
            if server.apply_err:
                raise Exception(server.apply_err)
                
            cmd1, ok = server.Logs(index)
            if ok:
                if count > 0 and cmd != cmd1:
                    raise Exception(f"Committed values at index {index} do not match")
                count += 1
                cmd = cmd1
                
        return count, cmd
        
    def one(self, cmd: Any, expected_servers: int, retry: bool = True) -> int:
        """
        Complete agreement on command - matching one() from test.go
        
        Submit command and wait for agreement by expected_servers.
        Returns index where command was committed.
        """
        start_time = time.time()
        starts = 0
        
        while time.time() - start_time < 10 and not self.finished:
            # Try all servers to find leader
            index = -1
            for _ in range(len(self.servers)):
                starts = (starts + 1) % len(self.servers)
                
                if self._is_connected(starts):
                    rf = self.servers[starts].RaftInstance()
                    if rf:
                        index1, _, ok = rf.Start(cmd)
                        if ok:
                            index = index1
                            break
            #logger.info(f"Leader found at {index}")
            if index != -1:
                # Wait for agreement
                wait_start = time.time()
                while time.time() - wait_start < 2:
                    nd, cmd1 = self.n_committed(index)
                    #logger.info(f"Checking agreement for cmd={cmd} {cmd1}, index={index}: found {nd}, expected {expected_servers}")
                    if nd > 0 and nd >= expected_servers:
                        if cmd1 == cmd:
                            return index
                    time.sleep(0.02)
                    
                if not retry:
                    raise Exception(f"one({cmd}) failed to reach agreement")
            else:
                time.sleep(0.05)
                
        raise Exception(f"one({cmd}) failed to reach agreement")
        
    def wait(self, index: int, n: int, start_term: int) -> Any:
        """Wait for n servers to commit index - matching wait() from test.go"""
        timeout = 0.01  # Start with 10ms
        
        for iters in range(30):
            nd, cmd = self.n_committed(index)
            if nd >= n:
                break
                
            time.sleep(timeout)
            if timeout < 1.0:
                timeout *= 2
                
            if start_term > -1:
                for server in self.servers:
                    rf = server.RaftInstance()
                    if rf:
                        t, _ = rf.GetState()
                        if t > start_term:
                            return -1  # Term changed
                            
        nd, cmd = self.n_committed(index)
        if nd < n:
            raise Exception(f"Only {nd} decided for index {index}; wanted {n}")
            
        return cmd
        
    def _is_connected(self, server: int) -> bool:
        """Check if server is connected to network"""
        return self.connected[server]
        
    # Enhanced network control methods - matching Go lab
    def disconnect(self, server: int):
        """Disconnect server from network - matching Go lab"""
        logger.debug(f"Disconnecting server {server}")
        self.connected[server] = False
        for i in range(self.n):
            self.net.Enable(f"end_{server}_{i}", False)  # Server can't send
            self.net.Enable(f"end_{i}_{server}", False)  # Server can't receive
            
    def connect(self, server: int):
        """Connect server to network - matching Go lab"""
        logger.debug(f"Connecting server {server}")
        self.connected[server] = True
        for i in range(self.n):
            self.net.Enable(f"end_{server}_{i}", True)   # Server can send
            self.net.Enable(f"end_{i}_{server}", True)   # Server can receive
        
    def set_unreliable(self, unreliable: bool):
        """Set network reliability - matching Go lab"""
        self.net.Reliable(not unreliable)
        
    def set_long_reordering(self, yes: bool):
        """Enable long message reordering - matching Go lab"""
        self.net.LongReordering(yes)
        
    def set_long_delays(self, yes: bool):
        """Enable long delays - matching Go lab"""
        self.net.LongDelays(yes)
        
    def bytes_total(self) -> int:
        """Get total bytes sent - matching Go lab BytesTotal()"""
        return self.net.GetTotalBytes()
        
    def rpc_count(self, server: int) -> int:
        """Get RPC count for specific server - matching Go lab"""
        return self.net.GetCount(server)
        
    def rpc_total(self) -> int:
        """Get total RPC count - matching Go lab"""
        return self.net.GetTotalCount()
        
    def cleanup(self):
        """Cleanup test resources - matching Go lab"""
        self.finished = True
        for server in self.servers:
            server.Kill()
        self.net.Cleanup()


# Example usage showing the server layer
def test_server_layer():
    """Demonstrate the server layer functionality"""
    print("Testing Raft Server Layer with labrpc...")
    
    # Create test configuration with 3 servers
    config = TestConfig(3, reliable=True, snapshot=False)
    
    # Wait for leader election
    time.sleep(2)
    leader = config.check_one_leader()
    print(f"Leader elected: {leader}")
    
    # Submit commands through server layer
    for i in range(5):
        index = config.one(f"command_{i}", 3)
        print(f"Command {i} committed at index {index}")
        
    # Check log consistency across servers
    print("\nLog contents across servers:")
    for i, server in enumerate(config.servers):
        log_entries = []
        j = 1
        while True:
            value, ok = server.Logs(j)
            if not ok:
                break
            log_entries.append(value)
            j += 1
        print(f"Server {i}: {log_entries}")
        
    config.cleanup()
    print("Server layer test completed")


if __name__ == "__main__":
    test_server_layer()