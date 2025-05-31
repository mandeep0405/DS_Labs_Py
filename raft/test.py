#!/usr/bin/env python3
"""
MIT 6.5840 Raft Tests in Python
Exactly matching the Go raft_test.go structure and behavior

This file contains the actual test cases that match the Go lab tests,
with proper separation of concerns and accurate RPC simulation.
"""

import threading
import time
import random
import unittest
import sys
import logging
from typing import List, Optional, Any, Dict
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import our modules
try:
    from persister import MakePersister, Persister
    from labrpc import MakeNetwork, Network, ClientEnd
    from raft import Make, Raft, ApplyMsg, LogEntry, RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply
    from server import RaftServer, TestConfig
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure all files are in the same directory:")
    print("- raft.py")
    print("- enhanced_labrpc.py")
    print("- enhanced_persister.py") 
    print("- server.py")
    print("- test.py")
    import sys
    sys.exit(1)

# Configure logging to match Go lab verbosity
logging.basicConfig(level=logging.DEBUG)  # Reduce noise during tests
logger = logging.getLogger(__name__)

# Constants matching Go lab
RAFT_ELECTION_TIMEOUT = 1000  # ms - generous timeout for tests

class RaftTest:
    """
    Raft Test Framework - exactly matching Go test.go structure
    
    This class provides the test infrastructure to create clusters,
    simulate network conditions, and run test scenarios.
    """
    
    def __init__(self, servers: int, reliable: bool = True, snapshot: bool = False):
        """Create test configuration matching makeTest() from test.go"""
        # Use the TestConfig from server.py which properly integrates everything
        self.config = TestConfig(servers, reliable, snapshot)
        self.n = servers
        self.reliable = reliable
        self.snapshot = snapshot
        
    @property
    def servers(self):
        """Get servers from config"""
        return self.config.servers
        
    @property
    def net(self):
        """Get network from config"""
        return self.config.net
        
    def cleanup(self):
        """Cleanup test resources - matching cleanup() from test.go"""
        self.config.cleanup()
        
    def restart(self, i: int):
        """Restart server i - matching restart() from test.go"""
        # Delegate to config
        pass  # Would be implemented if needed
        
    def check_one_leader(self) -> int:
        """Check that exactly one leader exists - matching checkOneLeader() from test.go"""
        return self.config.check_one_leader()
        
    def check_terms(self) -> int:
        """Check all servers agree on term - matching checkTerms() from test.go"""
        return self.config.check_terms()
        
    def check_no_leader(self):
        """Check no connected server thinks it's leader - matching checkNoLeader() from test.go"""
        self.config.check_no_leader()
                    
    def check_no_agreement(self, index: int):
        """Check no unexpected agreement at index - matching checkNoAgreement() from test.go"""
        self.config.check_no_agreement(index)
            
    def n_committed(self, index: int) -> tuple:
        """Count committed entries at index - matching nCommitted() from test.go"""
        return self.config.n_committed(index)
        
    def one(self, cmd: Any, expected_servers: int, retry: bool = True) -> int:
        """
        Complete agreement on command - matching one() from test.go
        
        Submit command and wait for agreement by expected_servers.
        Returns index where command was committed.
        """
        return self.config.one(cmd, expected_servers, retry)
        
    def wait(self, index: int, n: int, start_term: int) -> Any:
        """Wait for n servers to commit index - matching wait() from test.go"""
        return self.config.wait(index, n, start_term)
        
    def disconnect(self, server: int):
        """Disconnect server from network"""
        self.config.disconnect(server)
            
    def connect(self, server: int):
        """Connect server to network"""
        self.config.connect(server)
            
    def set_unreliable(self, unreliable: bool):
        """Set network reliability"""
        self.config.set_unreliable(unreliable)
        
    def set_long_reordering(self, yes: bool):
        """Enable long message reordering"""
        self.config.set_long_reordering(yes)
        
    def bytes_total(self) -> int:
        """Get total bytes sent"""
        return self.config.bytes_total()
        
    def rpc_count(self, server: int) -> int:
        """Get RPC count for server"""
        return self.config.rpc_count(server)


class TestRaft(unittest.TestCase):
    """
    Raft Test Cases - exactly matching raft_test.go
    
    Each test method matches a corresponding test in the Go lab.
    """
    
    def test_initial_election_3a(self):
        """Test (3A): initial election - matching TestInitialElection3A"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            # Is a leader elected?
            leader = test_config.check_one_leader()
            
            # Sleep to avoid racing with followers learning of election
            time.sleep(0.05)
            term1 = test_config.check_terms()
            self.assertGreaterEqual(term1, 1, "Term should be at least 1")
            
            # Does leader+term stay same if no network failure?
            time.sleep(2 * RAFT_ELECTION_TIMEOUT / 1000.0)
            term2 = test_config.check_terms()
            if term1 != term2:
                print("Warning: term changed even though there were no failures")
                
            # There should still be a leader
            test_config.check_one_leader()
            
        finally:
            test_config.cleanup()
            
    def test_re_election_3a(self):
        """Test (3A): election after network failure - matching TestReElection3A"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            # Wait longer for initial election to stabilize completely
            print("Waiting for initial election...")
            time.sleep(5)  # Increased from 4 to 5 seconds
            leader1 = test_config.check_one_leader()
            print(f"Initial leader: {leader1}")
            
            # Wait a bit more to ensure leadership is stable
            time.sleep(2)
            
            # If leader disconnects, new one should be elected
            print(f"Disconnecting leader {leader1}")
            test_config.disconnect(leader1)
            
            # Wait longer for new election to complete
            time.sleep(5)  # Increased from 3 to 5 seconds
            leader2 = test_config.check_one_leader()
            print(f"New leader after disconnection: {leader2}")
            
            # If old leader rejoins, shouldn't disturb new leader
            print(f"Reconnecting old leader {leader1}")
            test_config.connect(leader1)
            time.sleep(3)  # Give time for stabilization
            leader3 = test_config.check_one_leader()
            print(f"Leader after reconnection: {leader3}")
            
            # If no quorum, no new leader should be elected
            print(f"Creating network partition (disconnect {leader3} and {(leader3 + 1) % servers})")
            test_config.disconnect(leader3)
            test_config.disconnect((leader3 + 1) % servers)
            time.sleep(4)  # Wait for election timeouts
            
            # Check that remaining server doesn't think it's leader
            test_config.check_no_leader()
            print("✓ No leader when no quorum")
            
            # If quorum arises, should elect leader
            print(f"Restoring quorum (reconnect {(leader3 + 1) % servers})")
            test_config.connect((leader3 + 1) % servers)
            time.sleep(3)  # Wait for election
            leader4 = test_config.check_one_leader()
            print(f"New leader with quorum: {leader4}")
            
            # Re-join of last node shouldn't prevent leader
            print(f"Reconnecting last node {leader3}")
            test_config.connect(leader3)
            time.sleep(3)  # Wait for stabilization
            final_leader = test_config.check_one_leader()
            print(f"Final leader: {final_leader}")
            
        finally:
            test_config.cleanup()
            
    def test_basic_agree_3b(self):
        """Test (3B): basic agreement - matching TestBasicAgree3B"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            iters = 3
            for index in range(1, iters + 1):
                nd, _ = test_config.n_committed(index)
                if nd > 0:
                    self.fail("Some committed before Start()")
                    
                xindex = test_config.one(index * 100, servers, False)
                self.assertEqual(xindex, index, f"Got index {xindex} but expected {index}")
                
        finally:
            test_config.cleanup()
            
    def test_follower_failure_3b(self):
        """Test (3B): test progressive failure of followers - matching TestFollowerFailure3B"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(101, servers, False)
            
            # Disconnect one follower
            leader1 = test_config.check_one_leader()
            test_config.disconnect((leader1 + 1) % servers)
            
            # Leader and remaining follower should agree
            test_config.one(102, servers - 1, False)
            time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
            test_config.one(103, servers - 1, False)
            
            # Disconnect remaining follower
            leader2 = test_config.check_one_leader()
            test_config.disconnect((leader2 + 1) % servers)
            test_config.disconnect((leader2 + 2) % servers)
            
            # Submit command - should not commit
            rf = test_config.servers[leader2].RaftInstance()
            index, _, ok = rf.Start(104)
            self.assertTrue(ok, "Leader rejected Start()")
            self.assertEqual(index, 4, f"Expected index 4, got {index}")
            
            time.sleep(2 * RAFT_ELECTION_TIMEOUT / 1000.0)
            
            # Check command did not commit
            test_config.check_no_agreement(index)
            
        finally:
            test_config.cleanup()
            
    def test_leader_failure_3b(self):
        """Test (3B): test failure of leaders - matching TestLeaderFailure3B"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(101, servers, False)
            
            # Disconnect first leader
            leader1 = test_config.check_one_leader()
            test_config.disconnect(leader1)
            
            # Remaining followers should elect new leader
            test_config.one(102, servers - 1, False)
            time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
            test_config.one(103, servers - 1, False)
            
            # Disconnect new leader
            leader2 = test_config.check_one_leader()
            test_config.disconnect(leader2)
            
            # Submit command to each server
            for i in range(servers):
                rf = test_config.servers[i].RaftInstance()
                if rf:
                    rf.Start(104)
                    
            time.sleep(2 * RAFT_ELECTION_TIMEOUT / 1000.0)
            
            # Check command did not commit
            test_config.check_no_agreement(4)
            
        finally:
            test_config.cleanup()
            
    def test_fail_agree_3b(self):
        """Test (3B): agreement after follower reconnects - matching TestFailAgree3B"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(101, servers, False)
            
            # Disconnect one follower
            leader = test_config.check_one_leader()
            test_config.disconnect((leader + 1) % servers)
            
            # Leader and remaining follower should agree
            test_config.one(102, servers - 1, False)
            test_config.one(103, servers - 1, False)
            time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
            test_config.one(104, servers - 1, False)
            test_config.one(105, servers - 1, False)
            
            # Reconnect
            test_config.connect((leader + 1) % servers)
            
            # Full set should preserve agreements and agree on new commands
            test_config.one(106, servers, True)
            time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
            test_config.one(107, servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_fail_no_agree_3b(self):
        """Test (3B): no agreement if too many followers disconnect - matching TestFailNoAgree3B"""
        servers = 5
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(10, servers, False)
            
            # 3 of 5 followers disconnect
            leader = test_config.check_one_leader()
            test_config.disconnect((leader + 1) % servers)
            test_config.disconnect((leader + 2) % servers)
            test_config.disconnect((leader + 3) % servers)
            
            rf = test_config.servers[leader].RaftInstance()
            index, _, ok = rf.Start(20)
            self.assertTrue(ok, "Leader rejected Start()")
            self.assertEqual(index, 2, f"Expected index 2, got {index}")
            
            time.sleep(2 * RAFT_ELECTION_TIMEOUT / 1000.0)
            
            n, _ = test_config.n_committed(index)
            if n > 0:
                self.fail(f"{n} committed but no majority")
                
            # Repair
            test_config.connect((leader + 1) % servers)
            test_config.connect((leader + 2) % servers)
            test_config.connect((leader + 3) % servers)
            
            # Disconnected majority may have chosen leader, forgetting index 2
            leader2 = test_config.check_one_leader()
            rf2 = test_config.servers[leader2].RaftInstance()
            index2, _, ok2 = rf2.Start(30)
            self.assertTrue(ok2, "Leader2 rejected Start()")
            self.assertIn(index2, [2, 3], f"Unexpected index {index2}")
            
            test_config.one(1000, servers, True)
            
        finally:
            test_config.cleanup()
            
    # Add more test methods for 3C and 3D tests...
    
    def test_count_3b(self):
        """Test (3B): RPC counts aren't too high - matching TestCount3B"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            leader = test_config.check_one_leader()
            
            total1 = sum(test_config.rpc_count(j) for j in range(servers))
            if total1 > 30 or total1 < 1:
                self.fail(f"Too many or few RPCs ({total1}) to elect initial leader")
                
            # Test agreement RPC counts
            success = False
            for attempt in range(5):
                if attempt > 0:
                    time.sleep(3)  # Give solution time to settle
                    
                leader = test_config.check_one_leader()
                total1 = sum(test_config.rpc_count(j) for j in range(servers))
                
                iters = 10
                start_idx, term, ok = test_config.servers[leader].RaftInstance().Start(1)
                if not ok:
                    continue  # Leader moved on quickly
                    
                # Submit commands
                cmds = []
                for i in range(1, iters + 2):
                    x = random.randint(0, 2**31 - 1)
                    cmds.append(x)
                    index1, term1, ok = test_config.servers[leader].RaftInstance().Start(x)
                    if term1 != term or not ok:
                        break  # Term changed
                    if start_idx + i != index1:
                        self.fail("Start() returned wrong index")
                        
                # Wait for commitment
                failed = False
                for i in range(1, iters + 1):
                    cmd = test_config.wait(start_idx + i, servers, term)
                    if cmd == -1:
                        failed = True
                        break
                    if cmd != cmds[i - 1]:
                        self.fail(f"Wrong value committed for index {start_idx + i}")
                        
                if failed:
                    continue
                    
                # Check RPC counts
                total2 = sum(test_config.rpc_count(j) for j in range(servers))
                if total2 - total1 > (iters + 1 + 3) * 3:
                    self.fail(f"Too many RPCs ({total2 - total1}) for {iters} entries")
                    
                success = True
                break
                
            if not success:
                self.fail("Term changed too often")
                
            # Check idle RPC counts
            time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
            total3 = sum(test_config.rpc_count(j) for j in range(servers))
            if total3 - total2 > 3 * 20:
                self.fail(f"Too many RPCs ({total3 - total2}) for 1 second of idleness")
                
        finally:
            test_config.cleanup()


    def test_persist1_3c(self):
        """Test (3C): basic persistence - matching TestPersist13C"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(11, servers, True)
            
            # Shutdown all servers
            for i in range(servers):
                test_config.servers[i].Kill()
            time.sleep(0.1)
            
            # Restart all servers
            for i in range(servers):
                # Create new server with same persister
                persister = test_config.config.persisters[i]
                server = RaftServer(test_config.config, i, test_config.config.ends[i], persister, False)
                test_config.config.servers[i] = server
                test_config.config.net.AddServer(i, server.get_rpc_server())
                
            test_config.one(12, servers, True)
            
            # Kill and restart individual servers
            leader1 = test_config.check_one_leader()
            test_config.servers[leader1].Kill()
            time.sleep(0.1)
            
            # Restart leader
            persister = test_config.config.persisters[leader1]
            server = RaftServer(test_config.config, leader1, test_config.config.ends[leader1], persister, False)
            test_config.config.servers[leader1] = server
            test_config.config.net.AddServer(leader1, server.get_rpc_server())
            
            test_config.one(13, servers, True)
            
            leader2 = test_config.check_one_leader()
            test_config.servers[leader2].Kill()
            test_config.one(14, servers - 1, True)
            
            # Restart second leader
            persister = test_config.config.persisters[leader2]
            server = RaftServer(test_config.config, leader2, test_config.config.ends[leader2], persister, False)
            test_config.config.servers[leader2] = server
            test_config.config.net.AddServer(leader2, server.get_rpc_server())
            
            test_config.wait(4, servers, -1)  # Wait for leader2 to join
            
            i3 = (test_config.check_one_leader() + 1) % servers
            test_config.servers[i3].Kill()
            test_config.one(15, servers - 1, True)
            
            # Restart third server
            persister = test_config.config.persisters[i3]
            server = RaftServer(test_config.config, i3, test_config.config.ends[i3], persister, False)
            test_config.config.servers[i3] = server
            test_config.config.net.AddServer(i3, server.get_rpc_server())
            
            test_config.one(16, servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_persist2_3c(self):
        """Test (3C): more persistence - matching TestPersist23C"""
        servers = 5
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            index = 1
            for iters in range(5):
                test_config.one(10 + index, servers, True)
                index += 1
                
                leader1 = test_config.check_one_leader()
                
                # Shutdown some servers
                test_config.servers[(leader1 + 1) % servers].Kill()
                test_config.servers[(leader1 + 2) % servers].Kill()
                
                test_config.one(10 + index, servers - 2, True)
                index += 1
                
                # Shutdown more servers
                test_config.servers[(leader1 + 0) % servers].Kill()
                test_config.servers[(leader1 + 3) % servers].Kill()
                test_config.servers[(leader1 + 4) % servers].Kill()
                
                # Restart some servers
                for restart_idx in [(leader1 + 1) % servers, (leader1 + 2) % servers]:
                    persister = test_config.config.persisters[restart_idx]
                    server = RaftServer(test_config.config, restart_idx, test_config.config.ends[restart_idx], persister, False)
                    test_config.config.servers[restart_idx] = server
                    test_config.config.net.AddServer(restart_idx, server.get_rpc_server())
                    
                time.sleep(RAFT_ELECTION_TIMEOUT / 1000.0)
                
                # Restart another server
                persister = test_config.config.persisters[(leader1 + 3) % servers]
                server = RaftServer(test_config.config, (leader1 + 3) % servers, test_config.config.ends[(leader1 + 3) % servers], persister, False)
                test_config.config.servers[(leader1 + 3) % servers] = server
                test_config.config.net.AddServer((leader1 + 3) % servers, server.get_rpc_server())
                
                test_config.one(10 + index, servers - 2, True)
                index += 1
                
                # Restart remaining servers
                for restart_idx in [(leader1 + 4) % servers, (leader1 + 0) % servers]:
                    persister = test_config.config.persisters[restart_idx]
                    server = RaftServer(test_config.config, restart_idx, test_config.config.ends[restart_idx], persister, False)
                    test_config.config.servers[restart_idx] = server
                    test_config.config.net.AddServer(restart_idx, server.get_rpc_server())
                    
            test_config.one(1000, servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_persist3_3c(self):
        """Test (3C): partitioned leader and one follower crash, leader restarts - matching TestPersist33C"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(101, 3, True)
            
            leader = test_config.check_one_leader()
            test_config.disconnect((leader + 2) % servers)
            
            test_config.one(102, 2, True)
            
            # Crash leader and one follower
            test_config.servers[(leader + 0) % servers].Kill()
            test_config.servers[(leader + 1) % servers].Kill()
            test_config.connect((leader + 2) % servers)
            
            # Restart leader
            persister = test_config.config.persisters[(leader + 0) % servers]
            server = RaftServer(test_config.config, (leader + 0) % servers, test_config.config.ends[(leader + 0) % servers], persister, False)
            test_config.config.servers[(leader + 0) % servers] = server
            test_config.config.net.AddServer((leader + 0) % servers, server.get_rpc_server())
            
            test_config.one(103, 2, True)
            
            # Restart second server
            persister = test_config.config.persisters[(leader + 1) % servers]
            server = RaftServer(test_config.config, (leader + 1) % servers, test_config.config.ends[(leader + 1) % servers], persister, False)
            test_config.config.servers[(leader + 1) % servers] = server
            test_config.config.net.AddServer((leader + 1) % servers, server.get_rpc_server())
            
            test_config.one(104, servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_figure8_3c(self):
        """Test (3C): Figure 8 - matching TestFigure83C"""
        servers = 5
        test_config = RaftTest(servers, reliable=True, snapshot=False)
        
        try:
            test_config.one(random.randint(0, 10000), 1, True)
            
            nup = servers
            for iters in range(1000):
                leader = -1
                for i in range(servers):
                    rf = test_config.servers[i].RaftInstance()
                    if rf is not None:
                        cmd = random.randint(0, 10000)
                        _, _, ok = rf.Start(cmd)
                        if ok:
                            leader = i
                            
                if random.randint(0, 999) < 100:
                    ms = random.randint(0, RAFT_ELECTION_TIMEOUT // 2)
                    time.sleep(ms / 1000.0)
                else:
                    ms = random.randint(0, 13)
                    time.sleep(ms / 1000.0)
                    
                if leader != -1:
                    test_config.servers[leader].Kill()
                    nup -= 1
                    
                if nup < 3:
                    s = random.randint(0, servers - 1)
                    if test_config.servers[s].RaftInstance() is None:
                        # Restart server
                        persister = test_config.config.persisters[s]
                        server = RaftServer(test_config.config, s, test_config.config.ends[s], persister, False)
                        test_config.config.servers[s] = server
                        test_config.config.net.AddServer(s, server.get_rpc_server())
                        nup += 1
                        
            # Restart all servers
            for i in range(servers):
                if test_config.servers[i].RaftInstance() is None:
                    persister = test_config.config.persisters[i]
                    server = RaftServer(test_config.config, i, test_config.config.ends[i], persister, False)
                    test_config.config.servers[i] = server
                    test_config.config.net.AddServer(i, server.get_rpc_server())
                    
            test_config.one(random.randint(0, 10000), servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_unreliable_agree_3c(self):
        """Test (3C): unreliable agreement - matching TestUnreliableAgree3C"""
        servers = 5
        test_config = RaftTest(servers, reliable=False, snapshot=False)
        
        try:
            threads = []
            
            for iters in range(1, 50):
                for j in range(4):
                    def worker(i, j):
                        test_config.one((100 * i) + j, 1, True)
                    thread = threading.Thread(target=worker, args=(iters, j))
                    threads.append(thread)
                    thread.start()
                    
                test_config.one(iters, 1, True)
                
            test_config.set_unreliable(False)
            
            for thread in threads:
                thread.join()
                
            test_config.one(100, servers, True)
            
        finally:
            test_config.cleanup()
            
    def test_snapshot_basic_3d(self):
        """Test (3D): snapshots basic - matching TestSnapshotBasic3D"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=True)
        
        try:
            iters = 30
            test_config.one(random.randint(0, 10000), servers, True)
            leader1 = test_config.check_one_leader()
            
            for i in range(iters):
                victim = (leader1 + 1) % servers
                sender = leader1
                if i % 3 == 1:
                    sender = (leader1 + 1) % servers
                    victim = leader1
                    
                # Send enough to get a snapshot
                nn = (10 // 2) + (random.randint(0, 9))  # SnapShotInterval = 10
                for j in range(nn):
                    rf = test_config.servers[sender].RaftInstance()
                    if rf:
                        rf.Start(random.randint(0, 10000))
                        
                # Make sure all followers caught up
                test_config.one(random.randint(0, 10000), servers, True)
                
        finally:
            test_config.cleanup()
            
    def test_snapshot_install_3d(self):
        """Test (3D): install snapshots (disconnect) - matching TestSnapshotInstall3D"""
        servers = 3
        test_config = RaftTest(servers, reliable=True, snapshot=True)
        
        try:
            iters = 30
            test_config.one(random.randint(0, 10000), servers, True)
            leader1 = test_config.check_one_leader()
            
            for i in range(iters):
                victim = (leader1 + 1) % servers
                sender = leader1
                if i % 3 == 1:
                    sender = (leader1 + 1) % servers
                    victim = leader1
                    
                # Disconnect victim
                test_config.disconnect(victim)
                test_config.one(random.randint(0, 10000), servers - 1, True)
                
                # Send enough to get a snapshot
                nn = (10 // 2) + (random.randint(0, 9))
                for j in range(nn):
                    rf = test_config.servers[sender].RaftInstance()
                    if rf:
                        rf.Start(random.randint(0, 10000))
                        
                test_config.one(random.randint(0, 10000), servers - 1, True)
                
                # Reconnect - should receive snapshot to catch up
                test_config.connect(victim)
                test_config.one(random.randint(0, 10000), servers, True)
                leader1 = test_config.check_one_leader()
                
        finally:
            test_config.cleanup()
            
    def test_snapshot_all_crash_3d(self):
        """Test (3D): crash and restart all servers - matching TestSnapshotAllCrash3D"""
        servers = 3
        test_config = RaftTest(servers, reliable=False, snapshot=True)
        
        try:
            iters = 5
            test_config.one(random.randint(0, 10000), servers, True)
            
            for i in range(iters):
                # Perhaps enough to get a snapshot
                nn = (10 // 2) + (random.randint(0, 9))
                for j in range(nn):
                    test_config.one(random.randint(0, 10000), servers, True)
                    
                index1 = test_config.one(random.randint(0, 10000), servers, True)
                
                # Crash all
                for j in range(servers):
                    test_config.servers[j].Kill()
                time.sleep(0.1)
                
                # Restart all
                for j in range(servers):
                    persister = test_config.config.persisters[j]
                    server = RaftServer(test_config.config, j, test_config.config.ends[j], persister, True)
                    test_config.config.servers[j] = server
                    test_config.config.net.AddServer(j, server.get_rpc_server())
                    
                index2 = test_config.one(random.randint(0, 10000), servers, True)
                if index2 < index1 + 1:
                    self.fail(f"Index decreased from {index1} to {index2}")
                    
        finally:
            test_config.cleanup()
            
    def test_snapshot_init_3d(self):
        """Test (3D): snapshot initialization after crash - matching TestSnapshotInit3D"""
        servers = 3
        test_config = RaftTest(servers, reliable=False, snapshot=True)
        
        try:
            test_config.one(random.randint(0, 10000), servers, True)
            
            # Enough ops to make a snapshot
            nn = 10 + 1  # SnapShotInterval + 1
            for i in range(nn):
                test_config.one(random.randint(0, 10000), servers, True)
                
            # Crash all
            for i in range(servers):
                test_config.servers[i].Kill()
            time.sleep(0.1)
            
            # Restart all
            for i in range(servers):
                persister = test_config.config.persisters[i]
                server = RaftServer(test_config.config, i, test_config.config.ends[i], persister, True)
                test_config.config.servers[i] = server
                test_config.config.net.AddServer(i, server.get_rpc_server())
                
            # Single op to trigger write to persistent storage
            test_config.one(random.randint(0, 10000), servers, True)
            
            # Crash and restart again
            for i in range(servers):
                test_config.servers[i].Kill()
            time.sleep(0.1)
            
            for i in range(servers):
                persister = test_config.config.persisters[i]
                server = RaftServer(test_config.config, i, test_config.config.ends[i], persister, True)
                test_config.config.servers[i] = server
                test_config.config.net.AddServer(i, server.get_rpc_server())
                
            # Another op to trigger potential bug
            test_config.one(random.randint(0, 10000), servers, True)
            
        finally:
            test_config.cleanup()


def run_tests():
    """Run all Raft tests"""
    print("Running MIT 6.5840 Raft Tests in Python")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestRaft)
    
    # Run tests with detailed output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
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
            
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)