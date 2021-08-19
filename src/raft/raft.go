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
	HeartBeatTimeout        = (MinElectionTimeout / 10) * time.Millisecond
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
	rpcManager RaftRPCManager
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	term := rf.currentTerm
	role := rf.role
	reply.Term = term
	reply.VoteGranted = false

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

	if term > args.Term {
		rf.mu.Unlock()
		return
	}

	if term < args.Term {
		rf.role = RoleFollower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandiateID
		reply.VoteGranted = true
	}

	rf.mu.Unlock()
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

// rpc Implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	term := rf.currentTerm
	role := rf.role
	reply.Term = term

	// stale result
	if term > args.Term {
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	// update currentTerm and convert to follower
	if term < args.Term {
		rf.currentTerm = args.Term
		rf.role = RoleFollower
	}

	defer func() {
		params := hbParams{
			appendEntriesArgs:  *args,
			appendEntriesReply: *reply,
		}

		go func() {
			select {
			case rf.hbChan <- params:
			case <-time.After(HeartBeatTimeout):
				DPrintf("%s %d timeout sending heartbeat result at term %d", role, rf.me, term)
			}
		}()
	}()

	// a leagal heartbeat msg
	// TODO append entries
	reply.Success = true
	rf.mu.Unlock()
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
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
			rf.mu.Unlock()
			eto := getElectionTimeout()
			rf.runCandidate(term, eto)

		case RoleLeader:
			rf.mu.Unlock()
			rf.runLeader(term)

		default:
			panic(fmt.Sprintf("unknown raft role: %s", rf.role))
		}
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutInterval)+MinElectionTimeout) * time.Millisecond
}

func getHeartBeatTimeout() time.Duration {
	return HeartBeatTimeout * time.Millisecond
}

// update time out by:
// to = to - (time.Now() - start)
func updateTO(to *time.Duration, start time.Time) {
	*to -= time.Now().Sub(start)
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

	case params := <-rf.rvChan:
		// stale request vote msg
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

func (rf *Raft) startElection(term int, voteResultChan chan struct{}) {
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
			LastLogIndex: 0,    // TODO
			LastLogTerm:  term, // TODO
		}
		reply := &RequestVoteReply{}
		go func(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
			ok := rf.rpcManager.SendRequestVote(server, args, reply)
			if !ok {
				return
			}

			if reply.VoteGranted {
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
				}

				return
			}

			// check if we win the vote
			atomic.AddInt32(&vote, 1)
			if atomic.LoadInt32(&vote) > int32((size/2 + 1)) {
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

func (rf *Raft) runCandidate(term int, eto time.Duration) {
	start := time.Now()
	voteResultChan := make(chan struct{})

	// start election
	go rf.startElection(term, voteResultChan)

WAIT:
	select {
	case <-time.After(eto):
		// timeout without result, begin another election
	case <-voteResultChan:
		// else we are leader now
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm == term && rf.role == RoleCandidate {
			rf.role = RoleLeader
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

func (rf *Raft) sendHeartbeat(term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		args := &AppendEntriesArgs{
			Term:         term,
			LeaderID:     rf.me,
			PrevLogIndex: 0,   // TODO
			PrevLogTerm:  0,   // TODO
			Entries:      nil, // TODO
			LeaderCommit: 0,   // TODO
		}
		reply := &AppendEntriesReply{}
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			DPrintf("sending append entries rpc\n")
			ok := rf.rpcManager.SendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			if reply.Success || reply.Term <= term {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term {
				rf.role = RoleFollower
				rf.currentTerm = reply.Term
			}
		}(i, args, reply)
	}
}

func (rf *Raft) runLeader(term int) {
	start := time.Now()
	to := HeartBeatTimeout

	// send heartbeat
	go rf.sendHeartbeat(term)

WAIT:
	select {
	case <-time.After(to):
		// now send heart beat again

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

	// start ticker goroutine to start elections
	rf.role = RoleFollower
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	rf.rpcManager = &defaultRaftRPCManager{rf}
	go rf.ticker()

	return rf
}
