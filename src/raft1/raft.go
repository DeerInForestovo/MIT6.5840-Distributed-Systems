package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	// "bytes"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Persistent state of snapshot
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	// Volatile state
	votes       int
	state       State
	electionDue time.Time

	// channels
	applyCh chan raftapi.ApplyMsg

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// Return the last index of the log
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

// Convert global index to slice index
func (rf *Raft) toSliceIndex(index int) int {
	return index - rf.lastIncludedIndex
}

// helper function reset election timer
func (rf *Raft) resetElectionTimer() {
	timeout := time.Duration(100+rand.Intn(200)) * time.Millisecond
	rf.electionDue = time.Now().Add(timeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// Save Raft's persistent state.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

// Restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludedIndex, lastIncludedTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.snapshot = rf.persister.ReadSnapshot()

	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Term: rf.lastIncludedTerm})
	}
	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Take a snapshot of the state machine up to and including index.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}

	snapshotIndex := index
	snapshotTerm := rf.log[rf.toSliceIndex(snapshotIndex)].Term
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Term: snapshotTerm})
	if snapshotIndex < rf.lastIndex() {
		newLog = append(newLog, rf.log[rf.toSliceIndex(snapshotIndex+1):]...)
	}

	rf.log = newLog
	rf.lastIncludedIndex = snapshotIndex
	rf.lastIncludedTerm = snapshotTerm
	rf.snapshot = snapshot

	rf.persist()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// Follower install snapshot from leader.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.resetElectionTimer()

	if args.LastIncludedIndex > rf.lastIndex() {
		// Snapshot covers the whole log
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	} else {
		// Truncate existing log to start from snapshot
		sliceIdx := rf.toSliceIndex(args.LastIncludedIndex)
		rf.log = rf.log[sliceIdx:]
		rf.log[0].Term = args.LastIncludedTerm
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Snapshot

	rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)

	rf.persist()
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
}

// Leader send InstallSnapshot RPC to follower.
func (rf *Raft) sendInstallSnapshot(server int) {
	if rf.state != Leader {
		return
	}

	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          rf.snapshot,
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimer()
		return
	}

	rf.matchIndex[server] = max(rf.matchIndex[server], rf.lastIncludedIndex)
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
	go rf.sendAppendEntries(server, rf.currentTerm)
}

// RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If rf.currentTerm is newer, refuse voting
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If args.term is newer, become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
	}

	reply.Term = rf.currentTerm

	// Check if candidate log is at least as up-to-date as receiver
	lastLogIndex := rf.lastIndex()
	lastLogTerm := rf.log[rf.toSliceIndex(lastLogIndex)].Term
	candidateUpToDate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// Check whether vote or not
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidateUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	} else {
		reply.VoteGranted = false
	}

	rf.persist()
}

// Send requestVote to other servers.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Candidate starts election.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.votes = 1
	rf.resetElectionTimer()
	lastIndex := rf.lastIndex()
	lastTerm := rf.log[rf.toSliceIndex(lastIndex)].Term
	rf.persist()
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			var reply RequestVoteReply
			ok := rf.sendRequestVote(i, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()

			if rf.state != Candidate || term != rf.currentTerm {
				rf.mu.Unlock()
				return
			}

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.persist()
				rf.resetElectionTimer()
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				if rf.state == Candidate && term == rf.currentTerm {
					rf.votes++
					if rf.votes > len(rf.peers)/2 {
						rf.state = Leader
						for i := range rf.peers {
							rf.nextIndex[i] = rf.lastIndex() + 1
							rf.matchIndex[i] = rf.lastIncludedIndex
						}
						rf.resetElectionTimer()
						rf.mu.Unlock()
						rf.broadcastAppendEntries()
						return
					}
				}
			}
			rf.mu.Unlock()
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

// Server append entries to their log.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		// term is older
		return
	}

	rf.resetElectionTimer()

	if args.Term > rf.currentTerm {
		// find a leader to follow
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}

	if args.PrevLogIndex < rf.lastIncludedIndex {
		// log too old; need snapshot
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogIndex > rf.lastIndex() {
		// failed to append; more entries needed
		reply.ConflictIndex = rf.lastIndex() + 1
		reply.ConflictTerm = -1
		return
	}

	prevTerm := rf.lastIncludedTerm
	if args.PrevLogIndex != rf.lastIncludedIndex {
		prevTerm = rf.log[rf.toSliceIndex(args.PrevLogIndex)].Term
	}

	if prevTerm != args.PrevLogTerm {
		// prevLogTerm mismatch; find first index of conflict term
		conflictTerm := rf.log[args.PrevLogIndex].Term
		conflictIndex := args.PrevLogIndex
		for conflictIndex > 0 && rf.log[rf.toSliceIndex(conflictIndex-1)].Term == conflictTerm {
			conflictIndex--
		}
		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = conflictIndex
		return
	}

	if len(args.Entries) > 0 {
		newLogIndex := args.PrevLogIndex + 1
		for i, entry := range args.Entries {
			idx := newLogIndex + i
			if idx > rf.lastIndex() {
				// append any new entries that extend beyond current log
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			} else if rf.log[rf.toSliceIndex(idx)].Term != entry.Term {
				// delete from first conflict
				rf.log = rf.log[:rf.toSliceIndex(idx)]
				// append remaining entries
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		}
		rf.persist()
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

// Leader send the log to one follower server.
func (rf *Raft) sendAppendEntries(server int, term int) {
	rf.mu.Lock()

	if rf.state != Leader || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		go rf.sendInstallSnapshot(server)
		return
	}

	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.lastIncludedTerm
	prevLogSliceIndex := rf.toSliceIndex(prevLogIndex)
	if prevLogIndex != rf.lastIncludedIndex {
		prevLogTerm = rf.log[prevLogSliceIndex].Term
	}

	entries := make([]LogEntry, len(rf.log[prevLogSliceIndex+1:]))
	copy(entries, rf.log[prevLogSliceIndex+1:])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
		rf.resetElectionTimer()
		return
	}

	if rf.state != Leader || term != rf.currentTerm {
		return
	}

	if reply.Success {
		// update nextIndex & matchIndex
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// update commitIndex
		for N := rf.commitIndex + 1; N <= rf.lastIndex(); N++ {
			count := 1 // itself
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.log[rf.toSliceIndex(N)].Term == rf.currentTerm {
				rf.commitIndex = N
			}
		}
	} else {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			// search for the last index of conflict term in leader log
			conflictIndex := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					conflictIndex = i
					break
				}
			}
			if conflictIndex != -1 {
				rf.nextIndex[server] = conflictIndex + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
}

// Leader broadcast the log to all followers.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	term := rf.currentTerm
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, term)
	}
}

// Start agreement on next command
// return: (index, term, is-leader)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	entry := LogEntry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, entry)
	index := rf.lastIndex()
	rf.persist()

	go rf.broadcastAppendEntries()

	return index, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		if state == Leader {
			rf.broadcastAppendEntries()
			time.Sleep(100 * time.Millisecond)
		} else {
			rf.mu.Lock()
			if time.Now().After(rf.electionDue) {
				rf.resetElectionTimer()
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// Apply commited commands.
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.toSliceIndex(rf.lastApplied)].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// Init Raft.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		state:      Follower,
		votedFor:   -1,
		log:        make([]LogEntry, 0),
		applyCh:    applyCh,
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
	}
	rf.resetElectionTimer()
	rf.readPersist(persister.ReadRaftState())

	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 0)
		rf.log = append(rf.log, LogEntry{Term: 0})
	}

	go rf.ticker()
	go rf.applier()

	return rf
}
