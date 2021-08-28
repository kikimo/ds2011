package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type RaftRole string

const (
	RoleFollower  RaftRole = "follower"
	RoleCandidate RaftRole = "candidate"
	RoleLeader    RaftRole = "leader"
)

const (
	// unit: millsecond
	MinElectionTimeout      = 150
	MaxElectionTimeout      = 300
	HeartBeatTimeout        = ((MaxElectionTimeout + MinElectionTimeout) / 2 / 10) * time.Millisecond
	ElectionTimeoutInterval = MaxElectionTimeout - MinElectionTimeout
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// non-persistant field
	role       RaftRole
	hbChan     chan hbParams
	rvChan     chan rvParams
	applyCh    chan ApplyMsg
	rpcManager RaftRPCManager

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == RoleLeader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandiateID   int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type rvParams struct {
	requestVoteArgs  RequestVoteArgs
	requestVoteReply RequestVoteReply
}

type hbParams struct {
	appendEntriesArgs  AppendEntriesArgs
	appendEntriesReply AppendEntriesReply
}

// assume rf.mu lock hold
func (rf *Raft) compareLog(lastLogTerm int, lastLogIndex int) bool {
	sz := len(rf.log)
	ent := &rf.log[sz-1]

	if ent.Term < lastLogTerm {
		return true
	}

	if ent.Term == lastLogTerm {
		return ent.Index <= lastLogIndex
	}

	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	role := rf.role
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// no vote granted
	term := rf.currentTerm
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.role != RoleFollower {
			rf.role = RoleFollower
		}

		if rf.compareLog(args.LastLogTerm, args.LastLogIndex) {
			rf.votedFor = args.CandiateID + 1
			reply.VoteGranted = true
		}
	} else if rf.currentTerm == args.Term {
		if rf.votedFor == 0 && rf.compareLog(args.LastLogIndex, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandiateID + 1
		}
	} // else if rf.currentTerm > args.Term { /* do nothing */ }

	rf.mu.Unlock()
	defer func() {
		params := rvParams{
			requestVoteArgs:  *args,
			requestVoteReply: *reply,
		}

		go func() {
			select {
			case rf.rvChan <- params:
			case <-time.After(HeartBeatTimeout):
				DPrintf("%s %d timeout sending request vote result at term %d", role, rf.me, term)
			}
		}()
	}()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

// assume rf.mu lock hold
// TODO test me, watch out for log index
func (rf *Raft) getHBEntries() []*AppendEntriesArgs {
	sz := len(rf.peers)
	entries := make([]*AppendEntriesArgs, sz)
	for i := 0; i < sz; i++ {
		if i == rf.me {
			continue
		}

		offset := rf.log[0].Index
		nextIndex := rf.nextIndex[i]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.log[prevLogIndex-offset].Term
		logSz := len(rf.log) - nextIndex + offset
		// logSz := len(rf.log[nextIndex-offset:])
		log := make([]LogEntry, logSz)
		copy(log, rf.log[nextIndex-offset:])
		entries[i] = &AppendEntriesArgs{
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      log,
			LeaderCommit: rf.commitIndex,
		}
	}

	return entries
}

func minInt(i, j int) int {
	if i < j {
		return i
	}

	return j
}

// assume rf.mu lock hold
func (rf *Raft) appendLog(prevLogIndex int, entries []LogEntry) bool {
	offset := rf.log[0].Index
	sz := offset + len(rf.log)
	start := prevLogIndex + 1

	// start reach end of rf.log
	if start == sz {
		rf.log = append(rf.log, entries...)
		return true
	}

	esz := prevLogIndex + len(entries) + 1
	end := minInt(sz, esz)
	for i := start; i < end; i++ {
		j := i - start
		ent := &entries[j]
		if rf.log[i-offset].Term != ent.Term {
			// fmt.Printf("mismatch and cut at %dth, j: %d, start: %d\n", i, j, i-offset-1)
			rf.log = append(rf.log[:i-offset], entries[j:]...)
			return false
		}
	}

	if esz > end {
		rf.log = append(rf.log, entries[end-start:]...)
	}

	return true
}

// rpc Implementation
// TODO bisect optimization
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	role := rf.role
	reply.Term = rf.currentTerm
	reply.Success = false

	// stale request
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	// update currentTerm and convert to follower
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.role != RoleFollower {
			rf.role = RoleFollower
			rf.votedFor = 0
		}
	}

	// handle stale entries, very tricky
	// TODO unit test me
	// delete conflict entries
	// 如果没有 conflict 的话，必须保留后续的 entry, extreamly import
	offset := rf.log[0].Index
	prevLogIndex := args.PrevLogIndex
	// make sure that prevLogIndex is in the range of rf.log
	if prevLogIndex-offset < len(rf.log) {
		ent := rf.log[prevLogIndex-offset]
		prevLogTerm := ent.Term
		if prevLogTerm == args.PrevLogTerm {
			rf.appendLog(prevLogIndex, args.Entries)
			if args.LeaderCommit > rf.commitIndex {
				lastIndex := rf.log[0].Index + len(rf.log)
				rf.commitIndex = minInt(lastIndex, args.LeaderCommit)
				rf.tryUpdateCommitIndex()
			}

			reply.Success = true
		}
	}

	rf.mu.Unlock()
	defer func() {
		params := hbParams{
			appendEntriesArgs:  *args,
			appendEntriesReply: *reply,
		}

		go func() {
			select {
			case rf.hbChan <- params:
			case <-time.After(HeartBeatTimeout):
				DPrintf("%s %d timeout sending heartbeat result at term %d", role, rf.me, reply.Term)
			}
		}()
	}()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == RoleLeader
	if !isLeader {
		return -1, -1, false
	}

	index := rf.log[0].Index + len(rf.log)
	term := rf.currentTerm
	ent := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, ent)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.applyCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		role := rf.role
		term := rf.currentTerm

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		switch role {
		case RoleFollower:
			rf.mu.Unlock()
			eto := getElectionTimeout()
			rf.runFollower(term, eto)

		case RoleCandidate:
			rf.votedFor = rf.me
			rf.currentTerm += 1
			lastLogEnt := rf.log[len(rf.log)-1]
			lastLogIndex := lastLogEnt.Index
			lastLogTerm := lastLogEnt.Term
			rf.mu.Unlock()
			eto := getElectionTimeout()
			rf.runCandidate(term+1, eto, lastLogIndex, lastLogTerm)

		case RoleLeader:
			argsList := rf.getHBEntries()
			rf.mu.Unlock()
			sendHBChan := make(chan hbParams)
			rf.sendHeartbeat(term, sendHBChan, argsList, false)
			rf.runLeader(term, sendHBChan)

		default:
			panic(fmt.Sprintf("unknown raft role: %s", rf.role))
		}
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutInterval)+MinElectionTimeout) * time.Millisecond
}

// update time out by:
// to = to - (time.Now() - start)
func updateTO(to *time.Duration, start time.Time) {
	*to -= time.Since(start)
}

func (rf *Raft) runFollower(term int, eto time.Duration) {
	start := time.Now()

WAIT:
	select {
	case <-time.After(eto):
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm == term && rf.role == RoleFollower {
			rf.role = RoleCandidate
		}

	case params := <-rf.hbChan:
		// stale hb msg
		if params.appendEntriesArgs.Term < term {
			DPrintf("follower %d recieve stale hb msg at term %d: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}
		DPrintf("follower %d recieve hb msg from leader %d and return\n", rf.me, params.appendEntriesArgs.LeaderID)
		return

	case params := <-rf.rvChan:
		// stale request vote msg
		DPrintf("follower %d recieve rv param from %d: %+v\n", rf.me, params.requestVoteArgs.CandiateID, params)
		if params.requestVoteArgs.Term < term {
			DPrintf("follower %d recieve stale request vote msg at term %d: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}

		// vote not granted
		if !params.requestVoteReply.VoteGranted {
			DPrintf("follower %d recieve request vote msg at term %d, but vote not granted: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}
	}
}

type RaftRPCManager interface {
	SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool
}

type defaultRaftRPCManager struct {
	rf *Raft
}

func (r *defaultRaftRPCManager) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return r.rf.sendRequestVote(server, args, reply)
}

func (r *defaultRaftRPCManager) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return r.rf.sendAppendEntries(server, args, reply)
}

func (rf *Raft) startElection(term int, voteResultChan chan struct{}, lastLogIndex int, lastLogTerm int) {
	// func (rf *Raft) startElection(term int, voteResultChan chan struct{}) {
	var vote int32 = 1 // one vote from self
	size := len(rf.peers)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &RequestVoteArgs{
			CandiateID:   rf.me,
			Term:         term,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			DPrintf("candidate %d sending vote request to %d at term %d: %+v", rf.me, server, term, *args)
			ok := rf.rpcManager.SendRequestVote(server, args, reply)
			if !ok {
				return
			}
			DPrintf("candidate %d recieve vote result from %d: %+v", rf.me, server, *reply)

			if !reply.VoteGranted {
				DPrintf("candidate %d failed to get vote from %d at term %d", rf.me, server, term)
				if reply.Term <= term {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm < reply.Term {
					DPrintf("candidate %d convert to follower and update term from %d to %d", rf.me, rf.currentTerm, reply.Term)
					rf.currentTerm = reply.Term
					rf.role = RoleFollower
					rf.votedFor = 0
				}

				return
			}

			// check if we win the vote
			atomic.AddInt32(&vote, 1)
			votedGranted := atomic.LoadInt32(&vote)
			if votedGranted > int32((size / 2)) {
				select {
				case voteResultChan <- struct{}{}:
					DPrintf("candidate %d successful election result sent at term %d", rf.me, term)

				case <-time.After(HeartBeatTimeout):
					DPrintf("candidate %d timeout sending successful election result at term %d", rf.me, term)
				}
			}
		}(i, args, reply)
	}
}

// TODO unit test me
func (rf *Raft) runCandidate(term int, eto time.Duration, lastLogIndex int, lastLogTerm int) {
	start := time.Now()
	voteResultChan := make(chan struct{})

	// TODO split me to ease ut design of runCandidate
	go rf.startElection(term, voteResultChan, lastLogIndex, lastLogTerm)

WAIT:
	select {
	case <-time.After(eto):
		// timeout without result, begin another election
	case <-voteResultChan:
		// else we are leader now
		rf.mu.Lock()
		if rf.currentTerm == term && rf.role == RoleCandidate {
			DPrintf("candidate %d become leader at term %d", rf.me, rf.currentTerm)
			rf.role = RoleLeader
			// reinit nextIndex and matchIndex
			nextIndex := rf.log[0].Index + len(rf.log)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = nextIndex
				rf.matchIndex[i] = 0
			}

			// send empty heartbeat right away
			rf.mu.Unlock()
			sendHBChan := make(chan hbParams)
			rf.sendHeartbeat(term, sendHBChan, nil, true)
		} else {
			DPrintf("candidate %d not become leader at term %d(vote term %d)", rf.me, rf.currentTerm, term)
			rf.mu.Unlock()
		}

	case params := <-rf.hbChan:
		if params.appendEntriesArgs.Term < term {
			DPrintf("candidate %d recieve stale hb msg at term %d: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}

	case params := <-rf.rvChan:
		// stale request vote msg
		if params.requestVoteArgs.Term < term {
			DPrintf("candidate %d recieve stale request vote msg at term %d: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}

		// vote not granted
		if !params.requestVoteReply.VoteGranted {
			DPrintf("candidate %d recieve request vote msg at term %d, but vote not granted: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}
	}
}

func (rf *Raft) sendHeartbeat(term int, sendHBChan chan hbParams, appendArgsList []*AppendEntriesArgs, sendEmptyHB bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		var args *AppendEntriesArgs
		if !sendEmptyHB {
			args = appendArgsList[i]
		} else {
			args = &AppendEntriesArgs{}
		}
		args.Term = term
		args.LeaderID = rf.me

		reply := &AppendEntriesReply{}
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			DPrintf("leader %d sending append entries rpc\n", rf.me)
			ok := rf.rpcManager.SendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			if rf.currentTerm != term { // rf.currentTerm should have been updated
				rf.mu.Unlock()
				params := hbParams{
					appendEntriesReply: *reply,
				}

				select {
				case sendHBChan <- params:
				case <-time.After(HeartBeatTimeout):
					DPrintf("leader %d timeout sending role update msg from %d at term %d, new term is %d", rf.me, server, term, reply.Term)
				}

				return
			}

			// rf.currentTerm == term
			if term < reply.Term {
				rf.currentTerm = reply.Term
				rf.role = RoleFollower
				rf.votedFor = 0
				rf.mu.Unlock()
				params := hbParams{
					appendEntriesReply: *reply,
				}

				select {
				case sendHBChan <- params:
				case <-time.After(HeartBeatTimeout):
					DPrintf("leader %d timeout sending role update msg from %d at term %d, new term is %d", rf.me, server, term, reply.Term)
				}

				return
			}

			if sendEmptyHB {
				rf.mu.Unlock()
				return
			}

			// term >= reply.Term
			if reply.Success {
				// if rf.nextIndex[server] != args.PrevLogIndex+1, that means
				// this is a stale result and we should ignore
				if rf.nextIndex[server] == args.PrevLogIndex+1 {
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					rf.tryUpdateCommitIndex()
				}

				rf.mu.Unlock()
			} else {
				// log unmatch, decrease by one
				// TODO optimize index matching algorithm
				if rf.nextIndex[server] == args.PrevLogIndex+1 {
					rf.nextIndex[server]--
				}
				rf.mu.Lock()
			}
		}(i, args, reply)
	}
}

// assume rf.mu lock hold
// TODO unit test me
func (rf *Raft) tryUpdateCommitIndex() {
	sz := len(rf.matchIndex)
	matchIndex := make([]int, sz)
	copy(matchIndex, rf.matchIndex)
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] < matchIndex[j]
	})

	newCommitIndex := matchIndex[sz/2]
	if newCommitIndex > rf.commitIndex {
		offset := rf.log[0].Index
		ent := rf.log[newCommitIndex-offset]
		if ent.Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
			for rf.lastApplied < rf.commitIndex {
				index := rf.lastApplied + 1
				msg := ApplyMsg{
					CommandIndex: index,
					Command:      rf.log[index-offset],
				}
				rf.applyCh <- msg
				rf.lastApplied++
			}
		}
	}
}

func (rf *Raft) runLeader(term int, sendHBChan chan hbParams) {
	start := time.Now()
	to := HeartBeatTimeout

	// send heartbeat
	// go rf.sendHeartbeat(term)

WAIT:
	select {
	case <-time.After(to):
		// now send heart beat again

	case params := <-sendHBChan:
		if params.appendEntriesReply.Term > term {
			return
		}

		updateTO(&to, start)
		goto WAIT

	case params := <-rf.hbChan:
		if params.appendEntriesArgs.Term < term {
			DPrintf("leader %d recieve stale hb msg at term %d: %+v", rf.me, term, params)
			updateTO(&to, start)
			goto WAIT
		}

	case params := <-rf.rvChan:
		// stale request vote msg
		if params.requestVoteArgs.Term < term {
			DPrintf("leader %d recieve stale request vote msg at term %d: %+v", rf.me, term, params)
			updateTO(&to, start)
			goto WAIT
		}

		// vote not granted
		if !params.requestVoteReply.VoteGranted {
			DPrintf("leader %d recieve request vote msg at term %d, but vote not granted: %+v", rf.me, term, params)
			updateTO(&to, start)
			goto WAIT
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) == 0 {
		nullEnt := LogEntry{
			Index: 0,
			Term:  0,
		}
		rf.log = append(rf.log, nullEnt)
	}

	// start ticker goroutine to start elections
	rf.role = RoleFollower
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.rpcManager = &defaultRaftRPCManager{rf}
	go rf.ticker()

	return rf
}
