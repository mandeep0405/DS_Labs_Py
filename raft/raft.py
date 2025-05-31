#!/usr/bin/env python3
"""
MIT 6.5840 Raft Implementation in Python
FIXED to properly use labrpc network simulation

This implementation now correctly uses the enhanced_labrpc network
for all RPC communication, matching the Go lab behavior exactly.
"""

import threading
import time
import random
import pickle
import copy
from typing import List, Dict, Any, Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import logging
import queue

# Import enhanced modules
from persister import Persister
from labrpc import ClientEnd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

# Reduce noise from network layer
logging.getLogger('enhanced_labrpc').setLevel(logging.WARNING)

# Constants matching Go lab
ELECTION_TIMEOUT_MIN = 300  # ms - Increased from 150
ELECTION_TIMEOUT_MAX = 600  # ms - Increased from 300  
HEARTBEAT_INTERVAL = 50     # ms - Decreased from 100 for faster heartbeats

class State(Enum):
    """Raft node states - matching Go lab naming"""
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2

@dataclass
class LogEntry:
    """Log entry structure"""
    Term: int
    Command: Any

@dataclass
class ApplyMsg:
    """ApplyMsg structure"""
    CommandValid: bool
    Command: Any
    CommandIndex: int
    
    # For 2D (snapshots)
    SnapshotValid: bool = False
    Snapshot: bytes = b''
    SnapshotTerm: int = 0
    SnapshotIndex: int = 0

@dataclass
class RequestVoteArgs:
    """RequestVote RPC arguments"""
    Term: int
    CandidateId: int
    LastLogIndex: int
    LastLogTerm: int

@dataclass
class RequestVoteReply:
    """RequestVote RPC reply"""
    Term: int
    VoteGranted: bool

@dataclass
class AppendEntriesArgs:
    """AppendEntries RPC arguments"""
    Term: int
    LeaderId: int
    PrevLogIndex: int
    PrevLogTerm: int
    Entries: List[LogEntry]
    LeaderCommit: int

@dataclass
class AppendEntriesReply:
    """AppendEntries RPC reply"""
    Term: int
    Success: bool
    # Fast backup optimization
    ConflictIndex: int = -1
    ConflictTerm: int = -1

@dataclass
class InstallSnapshotArgs:
    """InstallSnapshot RPC arguments"""
    Term: int
    LeaderId: int
    LastIncludedIndex: int
    LastIncludedTerm: int
    Data: bytes

@dataclass
class InstallSnapshotReply:
    """InstallSnapshot RPC reply"""
    Term: int

class Raft:
    """
    Main Raft implementation 
    
    This class implements all parts of the MIT 6.5840 Raft labs:
    - Lab 3A: Leader Election
    - Lab 3B: Log Replication  
    - Lab 3C: Persistence
    - Lab 3D: Snapshots
    """
    
    def __init__(self, peers: List[ClientEnd], me: int, persister: Persister, applyCh: queue.Queue):
        """Initialize Raft peer"""
        # Basic identification - 
        self.peers = peers  # List of ClientEnd objects for RPC
        self.persister = persister
        self.me = me  # this peer's index into peers[]
        self.applyCh = applyCh  # channel to send ApplyMsg
        
        # Thread safety
        self.mu = threading.RLock()
        self.dead = 0  # atomic variable for killed status
        
        # =============================================================================
        # LAB 3C: PERSISTENCE - Persistent state on all servers (Figure 2)
        # =============================================================================
        self.currentTerm = 0
        self.votedFor = None 
        self.log = []  # log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
        
        # =============================================================================
        # LAB 3A/3B: VOLATILE STATE - Volatile state on all servers (Figure 2)
        # =============================================================================
        self.commitIndex = 0  # index of highest log entry known to be committed
        self.lastApplied = 0  # index of highest log entry applied to state machine
        
        # =============================================================================
        # LAB 3B: LEADER STATE - Volatile state on leaders (Figure 2) - reinitialized after election
        # =============================================================================
        self.nextIndex = []   # for each server, index of the next log entry to send to that server
        self.matchIndex = []  # for each server, index of highest log entry known to be replicated on server
        
        # =============================================================================
        # LAB 3A: ELECTION STATE - Additional state for leader election
        # =============================================================================
        self.state = State.FOLLOWER
        self.leaderId = None
        self.lastHeartbeat = time.time()
        self.electionTimeout = self._getElectionTimeout()
        
        # =============================================================================
        # LAB 3D: SNAPSHOT STATE - Snapshot state for log compaction
        # =============================================================================
        self.lastIncludedIndex = 0
        self.lastIncludedTerm = 0
        
        # Initialize log with dummy entry at index 0 (Go lab pattern)
        self.log.append(LogEntry(Term=0, Command=None))
        
        # LAB 3C: Read persistent state from storage
        self.readPersist(persister.ReadRaftState())
        
        # Start background threads
        threading.Thread(target=self._ticker, daemon=True).start()  # LAB 3A: Election timeouts, LAB 3B: Heartbeats
        threading.Thread(target=self._applier, daemon=True).start()  # LAB 3B: Apply committed entries
        
    # =============================================================================
    # LAB 3A: ELECTION TIMEOUT AND HEARTBEAT MANAGEMENT
    # =============================================================================
    
    def _getElectionTimeout(self) -> float:
        """Get randomized election timeout in seconds - LAB 3A"""
        # Much wider randomization to prevent simultaneous elections
        base_timeout = ELECTION_TIMEOUT_MIN + random.random() * (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN)
        
        # Add significant server-specific jitter (up to 200ms difference between servers)
        server_jitter =  random.randint(0, 150)
        
        # Total timeout with wide range: 300-750ms + server offset
        total_timeout = base_timeout + server_jitter
        return total_timeout / 1000.0
        
    def _ticker(self):
        """Background thread for election timeouts and heartbeats - LAB 3A + LAB 3B"""
        while not self.killed():
            time.sleep(0.1)  # 100ms check interval - less aggressive
            
            with self.mu:
                if self.state == State.LEADER:
                    # LAB 3B: Send heartbeats as leader (rate limited internally)
                    self._sendHeartbeats()
                elif time.time() - self.lastHeartbeat > self.electionTimeout:
                    # LAB 3A: Start election if timeout expired
                    self._startElection()

                    self.lastHeartbeat = time.time()  # Reset heartbeat time after starting election
                    self.electionTimeout = self._getElectionTimeout()  # Get new randomized timeout
                    
    # =============================================================================
    # LAB 3B: LOG APPLICATION TO STATE MACHINE
    # =============================================================================
    
    def _applier(self):
        """Background thread for applying committed entries - LAB 3B"""
        while not self.killed():
            time.sleep(0.01)
            
            with self.mu:
                if self.lastApplied < self.commitIndex:
                    # Apply entries from lastApplied+1 to commitIndex
                    start_idx = self.lastApplied + 1
                    end_idx = self.commitIndex
                    
                    # Get entries to apply
                    entries_to_apply = []
                    for i in range(start_idx, end_idx + 1):
                        if i > self.lastIncludedIndex:
                            log_idx = i - self.lastIncludedIndex
                            if log_idx > 0 and log_idx < len(self.log):
                                entry = self.log[log_idx]
                                if entry.Command is not None:
                                    entries_to_apply.append((i, entry))
                                
                    self.lastApplied = end_idx
                    
                    # Apply entries without holding lock
                    if entries_to_apply:
                        self.mu.release()
                        try:
                            for index, entry in entries_to_apply:
                                msg = ApplyMsg(
                                    CommandValid=True,
                                    Command=entry.Command,
                                    CommandIndex=index
                                )
                                self.applyCh.put(msg)
                        finally:
                            self.mu.acquire()

    # =============================================================================
    # LAB 3A + LAB 3B: BASIC RAFT INTERFACE
    # =============================================================================

    def GetState(self) -> Tuple[int, bool]:
        """Return currentTerm and whether this server believes it is the leader - LAB 3A"""
        with self.mu:
            term = self.currentTerm
            isleader = (self.state == State.LEADER)
        return term, isleader
        
    def Start(self, command: Any) -> Tuple[int, int, bool]:
        """
        Start agreement on a new log entry - LAB 3B
        Returns: (index, term, isLeader)
        """
        with self.mu:
            index = -1
            term = self.currentTerm
            isLeader = (self.state == State.LEADER)
            
            if not isLeader:
                return index, term, isLeader
                
            # Append entry to leader's log
            index = len(self.log) + self.lastIncludedIndex
            entry = LogEntry(Term=self.currentTerm, Command=command)
            self.log.append(entry)

            if not self.matchIndex:
                self.matchIndex = [0] * len(self.peers)
            self.matchIndex[self.me] = index
            
            # Persist the new log entry
            self.persist()
            
            # Start replication immediately
            self._sendHeartbeats()
            
            return index, term, isLeader
            
    # =============================================================================
    # LAB 3A: LEADER ELECTION IMPLEMENTATION
    # =============================================================================
            
    def _startElection(self):
        """Start a new election - LAB 3A"""
        self.state = State.CANDIDATE
        self.currentTerm += 1
        self.votedFor = self.me
        self.lastHeartbeat = time.time()
        self.electionTimeout = self._getElectionTimeout()
        
        # LAB 3C: Persist state change
        self.persist()
        
        logger.info(f"Node {self.me}: Starting election for term {self.currentTerm}")
        
        # Send RequestVote RPCs to all other servers
        args = RequestVoteArgs(
            Term=self.currentTerm,
            CandidateId=self.me,
            LastLogIndex=self._getLastLogIndex(),
            LastLogTerm=self._getLastLogTerm()
        )
        
        # Vote counting with proper synchronization
        votes_received = 1  # Vote for self
        votes_lock = threading.Lock()
        election_term = self.currentTerm  # Capture election term
        
        def send_vote_request(server_id):
            """Send vote request to specific server - LAB 3A"""
            reply = self.sendRequestVote(server_id, args)
            if reply is None:
                return
                
            with votes_lock:
                nonlocal votes_received
                with self.mu:
                    # Check if we're still a candidate for the same term
                    if (self.state != State.CANDIDATE or 
                        self.currentTerm != election_term or
                        self.currentTerm != args.Term):
                        return
                        
                    if reply.Term > self.currentTerm:
                        self._becomeFollower(reply.Term)
                        self.persist()  # LAB 3C: Persist state change
                        return
                        
                    if reply.VoteGranted and self.currentTerm == election_term:
                        votes_received += 1
                        logger.debug(f"Node {self.me}: Received vote from {server_id}, total votes: {votes_received}/{len(self.peers)}")
                        
                        # Check if we have majority and still candidate
                        majority = len(self.peers) // 2 + 1
                        if (votes_received >= majority and 
                            self.state == State.CANDIDATE and 
                            self.currentTerm == election_term):
                            logger.info(f"Node {self.me}: Won election with {votes_received}/{len(self.peers)} votes in term {election_term}")
                            self._becomeLeader()  # LAB 3A: Become leader with majority votes
                
        # Send vote requests in parallel using labrpc network
        for i in range(len(self.peers)):
            if i != self.me:
                threading.Thread(target=send_vote_request, args=(i,), daemon=True).start()
                
    def _becomeLeader(self):
        """Become leader - LAB 3A + LAB 3B"""
        # Double-check we're still a candidate (prevent race conditions)
        if self.state != State.CANDIDATE:
            return
            
        self.state = State.LEADER
        self.leaderId = self.me
        
        # LAB 3B: Initialize leader state (Figure 2)
        last_log_index = self._getLastLogIndex()
        self.nextIndex = [last_log_index + 1] * len(self.peers)
        self.matchIndex = [0] * len(self.peers)
        self.matchIndex[self.me] = last_log_index
        
        logger.info(f"Node {self.me}: Became leader for term {self.currentTerm}")
        
        # Send immediate burst of heartbeats to establish leadership quickly
        for _ in range(3):  # Send 3 rapid heartbeats
            self._sendHeartbeats()
            time.sleep(0.01)  # 10ms between heartbeats
        
    def _becomeFollower(self, term: int):
        """Become follower - LAB 3A"""
        old_term = self.currentTerm
        self.state = State.FOLLOWER
        self.currentTerm = term
        
        # CRITICAL: Only reset votedFor if term actually increased
        # If same term, keep our vote to prevent double voting
        if term > old_term:
            self.votedFor = None  # Reset vote only for new term
            
        #self.leaderId = None
        self.lastHeartbeat = time.time()  # Reset election timeout when becoming follower
        self.electionTimeout = self._getElectionTimeout()  # Get new random timeout
        
    # =============================================================================
    # LAB 3B: LOG REPLICATION IMPLEMENTATION
    # =============================================================================
        
    def _sendHeartbeats(self):
        """Send heartbeats to all followers - LAB 3B"""
        if self.state != State.LEADER:
            return
            
        # For new leaders, skip rate limiting to establish dominance quickly
        current_time = time.time()        
        self._last_heartbeat_time = current_time
        
        # Send to all followers
        for i in range(len(self.peers)):
            if i != self.me:
                threading.Thread(target=self._sendAppendEntries, args=(i,), daemon=True).start()
                
    def _sendAppendEntries(self, server: int):
        """Send AppendEntries RPC to a specific server - LAB 3B"""
        with self.mu:
            if self.state != State.LEADER:
                return
            
            if not hasattr(self, 'nextIndex') or not self.nextIndex:
                self.nextIndex = [self._getLastLogIndex() + 1] * len(self.peers)
            if not hasattr(self, 'matchIndex') or not self.matchIndex:
                self.matchIndex = [0] * len(self.peers)
                
            # LAB 3D: Check if we need to send InstallSnapshot instead
            if self.nextIndex[server] <= self.lastIncludedIndex:
                self._sendInstallSnapshot(server)
                return
                
            # Prepare AppendEntries arguments
            prevLogIndex = self.nextIndex[server] - 1
            prevLogTerm = 0
            
            if prevLogIndex == self.lastIncludedIndex:
                prevLogTerm = self.lastIncludedTerm
            elif prevLogIndex > self.lastIncludedIndex:
                log_idx = prevLogIndex - self.lastIncludedIndex - 1
                if log_idx >= 0 and log_idx < len(self.log):
                    prevLogTerm = self.log[log_idx].Term
                    
            # Prepare entries to send
            entries = []
            start_idx = self.nextIndex[server] - self.lastIncludedIndex
            if start_idx >= 0 and start_idx < len(self.log):
                entries = self.log[start_idx:]
                
            args = AppendEntriesArgs(
                Term=self.currentTerm,
                LeaderId=self.me,
                PrevLogIndex=prevLogIndex,
                PrevLogTerm=prevLogTerm,
                Entries=entries,
                LeaderCommit=self.commitIndex
            )
            
        # Send RPC using labrpc network
        reply = self.sendAppendEntries(server, args)
        
        # Handle reply
        with self.mu:
            if reply and self.state == State.LEADER and self.currentTerm == args.Term:
                self._handleAppendEntriesReply(server, args, reply)
                
    def _handleAppendEntriesReply(self, server: int, args: AppendEntriesArgs, reply: AppendEntriesReply):
        """Handle AppendEntries RPC reply - LAB 3B"""
        if reply.Term > self.currentTerm:
            self._becomeFollower(reply.Term)
            self.persist()  # LAB 3C: Persist state change
            return
            
        if reply.Success:
            # Update nextIndex and matchIndex for successful replication
            self.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
            self.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
            
            # Try to advance commitIndex based on majority replication
            self._updateCommitIndex()
        else:
            if reply.ConflictIndex > 0:
                self.nextIndex[server] = reply.ConflictIndex
            else:
                self.nextIndex[server] = max(1, self.nextIndex[server] - 1)
                
    def _updateCommitIndex(self):
        """Update commitIndex based on majority replication - LAB 3B"""
        if self.state != State.LEADER:
            return
            
        # Find highest index replicated on majority
        for n in range(self._getLastLogIndex(), self.commitIndex, -1):
            if n <= self.lastIncludedIndex:
                break
                
            count = 0
            for i in range(len(self.peers)):
                if self.matchIndex[i] >= n and self.matchIndex[i] >= n:
                    count += 1
                    
            if count > len(self.peers) // 2:
                # Only commit entries from current term
                log_idx = n - self.lastIncludedIndex 
                if log_idx >= 0 and log_idx < len(self.log) and self.log[log_idx].Term == self.currentTerm:
                    self.commitIndex = n
                    break
                    
    # =============================================================================
    # LAB 3A: REQUEST VOTE RPC HANDLER
    # =============================================================================
                    
    def RequestVote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """Handle RequestVote RPC - LAB 3A"""
        with self.mu:
            reply = RequestVoteReply(Term=self.currentTerm, VoteGranted=False)
            
            logger.debug(f"Node {self.me}: RequestVote from {args.CandidateId} for term {args.Term} (my term: {self.currentTerm}, votedFor: {self.votedFor})")
            
            # Reply false if term < currentTerm (§5.1)
            if args.Term < self.currentTerm:
                logger.debug(f"Node {self.me}: Denied vote to {args.CandidateId} - lower term ({args.Term} < {self.currentTerm})")
                return reply
                
            # Convert to follower if term is higher (§5.1)
            if args.Term > self.currentTerm:
                logger.debug(f"Node {self.me}: Higher term {args.Term}, becoming follower")
                self._becomeFollower(args.Term)
                reply.Term = self.currentTerm
                
            # CRITICAL: Can only vote if we haven't voted in this term OR voting for same candidate
            # This prevents double voting which causes split brain
            can_vote = (self.votedFor is None or self.votedFor == args.CandidateId)
            log_up_to_date = self._isLogUpToDate(args.LastLogIndex, args.LastLogTerm)
            
            if can_vote and log_up_to_date:
                self.votedFor = args.CandidateId
                reply.VoteGranted = True
                self.lastHeartbeat = time.time()  # Reset election timeout
                self.persist()  # LAB 3C: Persist vote decision
                logger.debug(f"Node {self.me}: Granted vote to {args.CandidateId} for term {args.Term}")
            else:
                reason = "already voted" if not can_vote else "log not up-to-date"
                logger.debug(f"Node {self.me}: Denied vote to {args.CandidateId} - {reason} (votedFor: {self.votedFor})")
                
            return reply
            
    # =============================================================================
    # LAB 3B: APPEND ENTRIES RPC HANDLER
    # =============================================================================
            
    def AppendEntries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """Handle AppendEntries RPC - LAB 3B"""
        with self.mu:
            reply = AppendEntriesReply(Term=self.currentTerm, Success=False)
            
            logger.debug(f"Node {self.me}: AppendEntries from {args.LeaderId} for term {args.Term} (my term: {self.currentTerm}, state: {self.state.name})")
            
            # Reply false if term < currentTerm (§5.1)
            if args.Term < self.currentTerm:
                return reply
            

            if args.Term >= self.currentTerm:
                self._becomeFollower(args.Term)  # Convert to follower if term is higher or equal
                self.leaderId = args.LeaderId
                reply.Term = self.currentTerm

                # Reset election timeout - we heard from leader (§5.2)
                self.lastHeartbeat = time.time()
                self.electionTimeout = self._getElectionTimeout()
                
            # Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
            if args.PrevLogIndex > self._getLastLogIndex():
                reply.ConflictIndex = self._getLastLogIndex() + 1
                reply.ConflictTerm = -1
                return reply
                
            if args.PrevLogIndex < self.lastIncludedIndex:
                reply.ConflictIndex = self.lastIncludedIndex + 1
                reply.ConflictTerm = -1
                return reply
            
            have_prev_entry = False
            if args.PrevLogIndex == 0:
                have_prev_entry = True
            elif args.PrevLogIndex == self.lastIncludedIndex:
                have_prev_entry = (args.PrevLogTerm == self.lastIncludedTerm)
            else:
                log_idx = args.PrevLogIndex - self.lastIncludedIndex
                if log_idx >= 0 and log_idx < len(self.log):
                    have_prev_entry = (self.log[log_idx].Term == args.PrevLogTerm)
        
            if not have_prev_entry and args.PrevLogIndex > 0:
                if args.PrevLogIndex > self.lastIncludedIndex:
                    log_idx = args.PrevLogIndex - self.lastIncludedIndex - 1
                    if log_idx >= len(self.log) or self.log[log_idx].Term != args.PrevLogTerm:
                        # Optimization: include conflict term and index
                        if log_idx < len(self.log):
                            reply.ConflictTerm = self.log[log_idx].Term
                            reply.ConflictIndex = self.lastIncludedIndex + 1  # Next index after conflict
                            # Find first index of this term
                            for i in range(1, len(self.log)):
                                if self.log[i].Term == reply.ConflictTerm:
                                    reply.ConflictIndex = i + self.lastIncludedIndex 
                                    break
                        else:
                            reply.ConflictIndex = self._getLastLogIndex() + 1
                        return reply
                    
            # If an existing entry conflicts with a new one (same index but different terms),
            # delete the existing entry and all that follow it
            if len(args.Entries) > 0:
                new_entries = False
                for i, entry in enumerate(args.Entries):
                    log_idx = args.PrevLogIndex + 1 + i - self.lastIncludedIndex

                    if log_idx >= len(self.log):
                        # Append new entry if it doesn't exist
                        self.log.append(entry)
                        new_entries = True
                    elif log_idx > 0 and self.log[log_idx].Term != entry.Term:
                        self.log = self.log[:log_idx]  # Truncate log at conflict index
                        self.log.append(entry)  # Append new entry
                        new_entries = True
                        
                # LAB 3C: Persist changes to log
                if new_entries:
                    self.persist()
            
            # If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
            if args.LeaderCommit > self.commitIndex:
                self.commitIndex = min(args.LeaderCommit, args.PrevLogIndex + len(args.Entries))
                
            reply.Success = True
            return reply
            
    # =============================================================================
    # LAB 3D: INSTALL SNAPSHOT RPC HANDLER
    # =============================================================================
            
    def InstallSnapshot(self, args: InstallSnapshotArgs) -> InstallSnapshotReply:
        """Handle InstallSnapshot RPC - LAB 3D"""
        with self.mu:
            reply = InstallSnapshotReply(Term=self.currentTerm)
            
            if args.Term < self.currentTerm:
                return reply
            
            if args.Term > self.currentTerm:
                self._becomeFollower(args.Term)
                reply.Term = self.currentTerm

            self.lastHeartbeat = time.time()
            self.leaderId = args.LeaderId
                
            # Install snapshot
            if args.LastIncludedIndex <= self.lastIncludedIndex:
                return reply  # Already have newer snapshot
                
            new_log = [LogEntry(Term=0, Command=None)]  # Start with dummy entry
            
            if (len(self.log) > 1 and 
                args.LastIncludedIndex > self._getLastLogIndex() and 
                args.LastIncludedIndex - self.lastIncludedIndex < len(self.log)):

                snapshot_idx = args.LastIncludedIndex - self.lastIncludedIndex 

                if snapshot_idx < len(self.log) and snapshot_idx > 0 and self.log[snapshot_idx].Term == args.LastIncludedTerm:
                    new_log = [LogEntry(Term=0, Command=None)]  # Reset log to just the snapshot entry
                    new_log.extend(self.log[snapshot_idx + 1:])  # Keep entries after snapshot

            # Update state
            self.lastIncludedIndex = args.LastIncludedIndex
            self.lastIncludedTerm = args.LastIncludedTerm
            self.log = new_log

            self.commitIndex = max(self.commitIndex, args.LastIncludedIndex)
            self.lastApplied = max(self.lastApplied, args.LastIncludedIndex)
            

            # LAB 3C + LAB 3D: Save state and snapshot to persistent storage
            self.persist()
            self.persister.Save(self._encodeState(), args.Data)
            
            # Send snapshot to application layer
            msg = ApplyMsg(
                CommandValid=False,
                Command=None,
                CommandIndex=0,
                SnapshotValid=True,
                Snapshot=args.Data,
                SnapshotTerm=args.LastIncludedTerm,
                SnapshotIndex=args.LastIncludedIndex
            )
            
            # Send without holding lock
            self.mu.release()
            try:
                self.applyCh.put(msg)
            finally:
                self.mu.acquire()
                
            return reply
            
    def Snapshot(self, index: int, snapshot: bytes):
        """Create a snapshot - called by service - LAB 3D"""
        with self.mu:
            if index <= self.lastIncludedIndex:
                return  # Already have newer snapshot
                
            # Find the term at the snapshot index
            log_idx = index - self.lastIncludedIndex - 1
            if log_idx >= len(self.log):
                return
                
            snapshot_term = self.log[log_idx].Term
            
            # Update snapshot state
            self.lastIncludedIndex = index
            self.lastIncludedTerm = snapshot_term
            
            # Compact log by removing entries covered by snapshot
            self.log = self.log[log_idx + 1:]
            
            # LAB 3C + LAB 3D: Save state and snapshot to persistent storage
            self.persister.Save(self._encodeState(), snapshot)
            
    # =============================================================================
    # LAB 3A + LAB 3B: UTILITY HELPER METHODS
    # =============================================================================
            
    def _isLogUpToDate(self, lastLogIndex: int, lastLogTerm: int) -> bool:
        """Check if candidate's log is at least as up-to-date - LAB 3A"""
        myLastTerm = self._getLastLogTerm()
        myLastIndex = self._getLastLogIndex()
        
        if lastLogTerm != myLastTerm:
            return lastLogTerm > myLastTerm
        return lastLogIndex >= myLastIndex
        
    def _getLastLogIndex(self) -> int:
        """Get index of last log entry"""
        return self.lastIncludedIndex + len(self.log) - 1
        
    def _getLastLogTerm(self) -> int:
        """Get term of last log entry"""
        if len(self.log) <= 1:
            return self.lastIncludedTerm  # LAB 3D: Return snapshot term if no log entries
        return self.log[-1].Term
        
    # =============================================================================
    # LAB 3C: PERSISTENCE IMPLEMENTATION
    # =============================================================================
        
    def persist(self):
        """Save persistent state to storage - LAB 3C"""
        data = self._encodeState()
        self.persister.SaveRaftState(data)
        
    def _encodeState(self) -> bytes:
        """Encode persistent state for storage - LAB 3C"""
        state = {
            'currentTerm': self.currentTerm,
            'votedFor': self.votedFor,
            'log': self.log,
            'lastIncludedIndex': self.lastIncludedIndex,
            'lastIncludedTerm': self.lastIncludedTerm
        }
        return pickle.dumps(state)
        
    def readPersist(self, data: bytes):
        """Restore persistent state from storage - LAB 3C"""
        if not data:
            return
            
        try:
            state = pickle.loads(data)
            # Restore persistent state variables
            self.currentTerm = state['currentTerm']
            self.votedFor = state['votedFor']
            self.log = state['log']
            # LAB 3D: Restore snapshot state if present
            self.lastIncludedIndex = state.get('lastIncludedIndex', 0)
            self.lastIncludedTerm = state.get('lastIncludedTerm', 0)
        except Exception as e:
            logger.error(f"Failed to read persistent state: {e}")
            
    # =============================================================================
    # RAFT LIFECYCLE MANAGEMENT
    # =============================================================================
            
    def Kill(self):
        """Kill the Raft instance - ALL LABS"""
        self.dead = 1
        
    def killed(self) -> bool:
        """Check if Raft instance is killed - ALL LABS"""
        return self.dead == 1
        
    # =============================================================================
    # RPC METHODS - USING LABRPC NETWORK (ALL LABS)
    # =============================================================================
    
    def sendRequestVote(self, server: int, args: RequestVoteArgs) -> Optional[RequestVoteReply]:
        """Send RequestVote RPC using labrpc network - LAB 3A"""
        if server >= len(self.peers) or server < 0:
            return None
            
        try:
            # Create reply object with proper structure
            reply = RequestVoteReply(Term=0, VoteGranted=False)
            
            # Use labrpc ClientEnd to make the call
            ok = self.peers[server].Call("Raft.RequestVote", args, reply)
            
            if ok:
                return reply
            else:
                return None
                
        except Exception as e:
            logger.debug(f"RequestVote RPC to {server} failed: {e}")
            return None
        
    def sendAppendEntries(self, server: int, args: AppendEntriesArgs) -> Optional[AppendEntriesReply]:
        """Send AppendEntries RPC using labrpc network - LAB 3B"""
        if server >= len(self.peers) or server < 0:
            return None
            
        try:
            # Create reply object with proper structure
            reply = AppendEntriesReply(Term=0, Success=False, ConflictIndex=-1, ConflictTerm=-1)
            
            # Use labrpc ClientEnd to make the call
            ok = self.peers[server].Call("Raft.AppendEntries", args, reply)
            
            if ok:
                return reply
            else:
                return None
                
        except Exception as e:
            logger.debug(f"AppendEntries RPC to {server} failed: {e}")
            return None
        
    def _sendInstallSnapshot(self, server: int):
        """Send InstallSnapshot RPC using labrpc network - LAB 3D"""
        with self.mu:
            args = InstallSnapshotArgs(
                Term=self.currentTerm,
                LeaderId=self.me,
                LastIncludedIndex=self.lastIncludedIndex,
                LastIncludedTerm=self.lastIncludedTerm,
                Data=self.persister.ReadSnapshot()
            )
            
        reply = self.sendInstallSnapshot(server, args)
        
        with self.mu:
            if reply and reply.Term > self.currentTerm:
                self._becomeFollower(reply.Term)
                self.persist()  # LAB 3C: Persist state change
            elif reply and self.state == State.LEADER:
                self.nextIndex[server] = self.lastIncludedIndex + 1
                
    def sendInstallSnapshot(self, server: int, args: InstallSnapshotArgs) -> Optional[InstallSnapshotReply]:
        """Send InstallSnapshot RPC using labrpc network - LAB 3D"""
        if server >= len(self.peers) or server < 0:
            return None
            
        try:
            # Create reply object with proper structure
            reply = InstallSnapshotReply(Term=0)
            
            # Use labrpc ClientEnd to make the call
            ok = self.peers[server].Call("Raft.InstallSnapshot", args, reply)
            
            if ok:
                return reply
            else:
                return None
                
        except Exception as e:
            logger.debug(f"InstallSnapshot RPC to {server} failed: {e}")
            return None

def Make(peers: List[ClientEnd], me: int, persister: Persister, applyCh: queue.Queue) -> Raft:
    """
    Create a Raft peer (ALL LABS 3A/3B/3C/3D)
    
    peers: array of ClientEnd objects for RPC (NOT just server IDs!)
    me: this peer's index into peers[]
    persister: enhanced persister object to hold this peer's persisted state
    applyCh: channel on which the tester or service expects Raft to send ApplyMsg messages
    """
    rf = Raft(peers, me, persister, applyCh)
    return rf