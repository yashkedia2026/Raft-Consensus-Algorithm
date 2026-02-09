# Raft Consensus Algorithm
A fault-tolerant distributed consensus protocol implementation in Go for CS351 - Distributed Systems.

## Overview
Implementation of the Raft consensus algorithm that manages a replicated log across a distributed cluster. Ensures all nodes agree on the same sequence of commands despite network partitions, message loss, and node failures.

## Features
- Leader Election - Randomized timeout-based election with term management (300-450ms)
- Log Replication - Consistent log propagation from leader to followers
- Fault Tolerance - Automatic recovery from node failures and network partitions
- Safety Guarantees - Committed entries never lost or overwritten
- RPC Communication - Thread-safe state management with mutex synchronization

## Architecture
### Server States
- Follower - Responds to RPCs from leaders and candidates
- Candidate - Requests votes during election
- Leader - Handles client requests and replicates log

### Key Components
```go
type Raft struct {
    currentTerm     int           // Current election term
    votedFor        int           // Candidate voted for in current term
    state           string        // "follower", "candidate", or "leader"
    log             []LogEntry    // Log entries (command + term)
    commitIndex     int           // Highest log entry known to be committed
    lastApplied     int           // Highest log entry applied to state machine
    nextIndex       []int         // Next log index to send to each follower
    matchIndex      []int         // Highest log entry replicated on each follower
}
```

## API
```go
// Create new Raft server
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft

// Get current term and leader status
func (rf *Raft) GetState() (int, bool)

// Start agreement on new log entry
func (rf *Raft) Start(command interface{}) (index, term, isLeader)
```

## How It Works
### Leader Election
- Followers timeout after not receiving heartbeats
- Transition to candidate, increment term, request votes
- Candidate with majority votes becomes leader

### Log Replication
- Leader receives command via Start()
- Leader appends to local log, sends AppendEntries RPC to followers
- Once majority confirms, leader commits and applies entry

### Consistency
- Election Safety: At most one leader per term
- Log Matching: Same index/term means identical preceding entries
- Leader Completeness: Committed entries present in all future leaders

## Implementation Details
- Election Timeout: 300-450ms randomized to prevent split votes
- Heartbeat Interval: 100ms to maintain leadership
- Conflict Resolution: Optimized log backtracking using term/index hints
- Concurrency: Coarse-grained locking with mutex synchronization

## Testing Scenarios
- Network partitions and cluster splits
- Leader crashes with automatic re-election
- Follower failures and recovery
- Concurrent elections with split votes
- Log conflicts and consistency checking

## Limitations
Does not include:
- Log compaction/snapshots
- Cluster membership changes
- Batched AppendEntries
- Persistent storage / crash recovery

## Resources
- Raft Paper - Original consensus algorithm
- Raft Visualization - Interactive protocol demo
- Students Guide to Raft - Implementation tips
- Course: CS351 Distributed Systems, Boston University
