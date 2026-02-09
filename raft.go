package raft

//
// This is an outline of the API that raft must expose to
// the service (or tester). See comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   Create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   Start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester) in the same server.
//

import (
	"cs351/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). Set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	stateFollower  = "follower"
	stateCandidate = "candidate"
	stateLeader    = "leader"
)

const (
	heartbeatInterval     = 100 * time.Millisecond
	electionTimeoutBase   = 300
	electionTimeoutJitter = 150
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	me    int                 // This peer's index into peers[]
	dead  int32               // Set by Kill()

	currentTerm       int
	votedFor          int
	state             string
	electionTimeout   time.Duration
	electionResetTime time.Time

	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg
}

// Return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == stateLeader
}

// Example RequestVote RPC arguments structure.
// Field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// Example RequestVote RPC reply structure.
// Field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// Example AppendEntries RPC arguments structure.
// Field names must start with capital letters!
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// Example AppendEntries RPC reply structure.
// Field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// Example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = stateFollower
		rf.resetElectionTimerLocked()
	}

	reply.Term = rf.currentTerm
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTermLocked()
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimerLocked()
	} else {
		reply.VoteGranted = false
	}
}

// Example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = stateFollower
	} else if rf.state != stateFollower {
		rf.state = stateFollower
	}

	rf.resetElectionTimerLocked()
	reply.Term = rf.currentTerm

	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		reply.ConflictTerm = -1
		rf.mu.Unlock()
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		rf.mu.Unlock()
		return
	}

	if len(args.Entries) > 0 {
		insertIndex := args.PrevLogIndex + 1
		i := 0
		for insertIndex < len(rf.log) && i < len(args.Entries) {
			if rf.log[insertIndex].Term != args.Entries[i].Term {
				rf.log = rf.log[:insertIndex]
				break
			}
			insertIndex++
			i++
		}
		if i < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		lastNew := len(rf.log) - 1
		if args.LeaderCommit < lastNew {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNew
		}
	}

	reply.Success = true
	applyNeeded := rf.commitIndex > rf.lastApplied
	rf.mu.Unlock()

	if applyNeeded {
		rf.applyCommitted()
	}
}

// Example code to send a RequestVote RPC to a server.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// Example code to send an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	if rf.state != stateLeader {
		term := rf.currentTerm
		rf.mu.Unlock()
		return -1, term, false
	}

	index := len(rf.log)
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.mu.Unlock()

	go rf.replicateLogs()

	return index, term, true
}

// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. Your code can use killed() to
// check whether Kill() has been called. The use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. Any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		timeSinceReset := time.Since(rf.electionResetTime)
		timeout := rf.electionTimeout
		rf.mu.Unlock()

		if state == stateLeader {
			rf.replicateLogs()
			time.Sleep(heartbeatInterval)
			continue
		}

		if timeSinceReset >= timeout {
			rf.startElection()
			continue
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = stateCandidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.resetElectionTimerLocked()
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTermLocked()
	rf.mu.Unlock()

	var votes int32 = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = stateFollower
				rf.votedFor = -1
				rf.resetElectionTimerLocked()
				rf.mu.Unlock()
				return
			}
			if rf.state != stateCandidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			if reply.VoteGranted {
				if atomic.AddInt32(&votes, 1) > int32(len(rf.peers)/2) {
					rf.mu.Lock()
					if rf.state == stateCandidate && rf.currentTerm == term {
						rf.becomeLeaderLocked()
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = stateLeader
	lastIndex := len(rf.log)
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIndex - 1
	rf.nextIndex[rf.me] = lastIndex
	go rf.replicateLogs()
}

func (rf *Raft) replicateLogs() {
	rf.mu.Lock()
	if rf.state != stateLeader {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateToFollower(i, currentTerm)
	}
}

func (rf *Raft) replicateToFollower(server int, term int) {
	rf.mu.Lock()
	if rf.state != stateLeader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[server]
	if nextIdx < 1 {
		nextIdx = 1
		rf.nextIndex[server] = 1
	}
	if nextIdx > len(rf.log) {
		nextIdx = len(rf.log)
		rf.nextIndex[server] = nextIdx
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := -1
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	entries := append([]LogEntry(nil), rf.log[nextIdx:]...)

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	if !rf.sendAppendEntries(server, &args, &reply) {
		return
	}

	rf.mu.Lock()
	if rf.state != stateLeader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = stateFollower
		rf.votedFor = -1
		rf.resetElectionTimerLocked()
		rf.mu.Unlock()
		return
	}

	applyNeeded := false
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
			if rf.log[n].Term != rf.currentTerm {
				continue
			}
			count := 1
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				break
			}
		}
		applyNeeded = rf.commitIndex > rf.lastApplied
	} else {
		if reply.ConflictTerm >= 0 {
			lastIndex := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndex = i
					break
				}
			}
			if lastIndex >= 0 {
				rf.nextIndex[server] = lastIndex + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
	rf.mu.Unlock()

	if applyNeeded {
		rf.applyCommitted()
	}
}

func (rf *Raft) applyCommitted() {
	for {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		rf.lastApplied++
		index := rf.lastApplied
		command := rf.log[index].Command
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
	}
}

func (rf *Raft) lastLogIndexAndTermLocked() (int, int) {
	lastIndex := len(rf.log) - 1
	lastTerm := 0
	if lastIndex >= 0 {
		lastTerm = rf.log[lastIndex].Term
	}
	return lastIndex, lastTerm
}

func (rf *Raft) resetElectionTimerLocked() {
	rf.electionTimeout = time.Duration(electionTimeoutBase+rand.Intn(electionTimeoutJitter)) * time.Millisecond
	rf.electionResetTime = time.Now()
}

// The service or tester wants to create a Raft server. The ports
// of all the Raft servers (including this one) are in peers[]. This
// server's port is peers[me]. All the servers' peers[] arrays
// have the same order. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		state:       stateFollower,
		log:         []LogEntry{{Term: 0}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		applyCh:     applyCh,
	}

	rf.mu.Lock()
	rf.resetElectionTimerLocked()
	lastIndex := len(rf.log)
	for i := range peers {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	go rf.ticker()

	return rf
}
