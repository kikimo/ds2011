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
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	MinElectionTimeout = 150
	MaxElectionTimeout = 300
	// the paper suggest idle period to be a magnitude small than election timeout
	// but the test limits us to send no more than 10 AppendEntries per second, setting
	// so we set idle period to 64ms(more than 10 AppendEntries per second, but it works).
	// LeaderIdlePeriod        = ((MaxElectionTimeout + MinElectionTimeout) / 2 / 10) * time.Millisecond
	// LeaderIdlePeriod        = 64 * time.Millisecond
	LeaderIdlePeriod        = 128 * time.Millisecond
	ElectionTimeoutInterval = MaxElectionTimeout - MinElectionTimeout

	// note: setting leader idle period equal to election time will greatly
	// ease the detection raft bug
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

type AppendResult struct {
	isLeader bool
	index    int
	term     int
}
type LogEntryWrapper struct {
	command interface{}
	retCh   chan AppendResult
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
	log         []*LogEntry
	logBuf      *BlockQueue

	snapshot []byte

	// non-persistant field
	role          RaftRole
	hbChan        chan hbParams
	rvChan        chan rvParams
	appendLogChan chan struct{}
	applyCh       chan ApplyMsg
	lastHB        time.Time
	rpcManager    RaftRPCManager

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
func (rf *Raft) persist(withSnapshot bool) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic(fmt.Sprintf("failed encoding currentTerm: %+v", err))
	}

	if err := e.Encode(rf.votedFor); err != nil {
		panic(fmt.Sprintf("failed encoding votedFor: %+v", err))
	}

	if err := e.Encode(rf.log); err != nil {
		panic(fmt.Sprintf("failed encoding rf.log: %+v", err))
	}
	data := w.Bytes()

	if !withSnapshot {
		rf.persister.SaveRaftState(data)
	} else {
		rf.persister.SaveStateAndSnapshot(data, rf.snapshot)
	}
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.currentTerm); err != nil {
		panic(fmt.Sprintf("failed decoding currentTerm: %+v", err))
	}

	if err := d.Decode(&rf.votedFor); err != nil {
		panic(fmt.Sprintf("failed decoding votedFor: %+v", err))
	}

	if err := d.Decode(&rf.log); err != nil {
		panic(fmt.Sprintf("failed decoding log: %+v", err))
	}
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
// test whether lastLogTerm/lastLogIndex is newer or equal than the latest log ent.
func (rf *Raft) compareLog(lastLogTerm int, lastLogIndex int) bool {
	sz := len(rf.log)
	ent := rf.log[sz-1]

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
			DPrintf("follower %d grant vote to %d at term %d", rf.me, args.CandiateID, rf.currentTerm)
		}
		rf.persist(false)
	} else if rf.currentTerm == args.Term {
		DPrintf("server %d comparing log size %d with lastLogTerm %d, lastLogIndex %d", rf.me, len(rf.log)+rf.log[0].Index, args.LastLogTerm, args.LastLogIndex)
		if rf.votedFor == 0 && rf.compareLog(args.LastLogTerm, args.LastLogIndex) {
			reply.VoteGranted = true
			rf.votedFor = args.CandiateID + 1
			rf.persist(false)
			DPrintf("follower %d grant vote to %d at term %d", rf.me, args.CandiateID, rf.currentTerm)
		}
	} else if rf.currentTerm > args.Term { // ignore stale vote request
		rf.mu.Unlock()
		return
	}

	rf.mu.Unlock()
	params := rvParams{
		requestVoteArgs:  *args,
		requestVoteReply: *reply,
	}

	select {
	case rf.rvChan <- params:
	case <-time.After(MinElectionTimeout * time.Millisecond):
		DPrintf("%s %d timeout sending request vote result at term %d", role, rf.me, term)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success         bool
	Term            int
	NextTryLogIndex int
	NextTryLogTerm  int
}

// assume rf.mu lock hold
// TODO test me, watch out for log index
func (rf *Raft) getHBEntries() []*AppendEntriesArgs {
	DPrintf("log size of leader %d at term %d is: %d, offset: %d nextIndex: %+v, matchIndex: %+v", rf.me, rf.currentTerm, len(rf.log), rf.log[0].Index, rf.nextIndex, rf.matchIndex)
	sz := len(rf.peers)
	entries := make([]*AppendEntriesArgs, sz)
	for i := 0; i < sz; i++ {
		if i == rf.me {
			continue
		}

		DPrintf("leader %d copying log for %d", rf.me, i)
		offset := rf.log[0].Index
		// check underflow, rf.log[0] might have changed after raft take a new snapshot
		if rf.nextIndex[i]-offset <= 0 {
			rf.nextIndex[i] = offset + 1
		}

		nextIndex := rf.nextIndex[i]
		prevLogIndex := nextIndex - 1
		prevLogTerm := rf.log[prevLogIndex-offset].Term
		logSz := len(rf.log) - nextIndex + offset
		// logSz := len(rf.log[nextIndex-offset:])
		ent := &AppendEntriesArgs{
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			LeaderCommit: rf.commitIndex,
		}

		DPrintf("leader %d copying log for %d calling make() && copy()", rf.me, i)
		if logSz > 0 {
			ent.Entries = make([]*LogEntry, logSz)
			copy(ent.Entries, rf.log[nextIndex-offset:])
		}
		entries[i] = ent
		DPrintf("leader %d finish copying log for %d", rf.me, i)
	}
	DPrintf("leader %d finish preparing append entries at term %d", rf.me, rf.currentTerm)

	return entries
}

func minInt(i, j int) int {
	if i < j {
		return i
	}

	return j
}

// assume rf.mu lock hold
func (rf *Raft) appendLog(prevLogIndex int, entries []*LogEntry) bool {
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
		ent := entries[j]
		if rf.log[i-offset].Term != ent.Term {
			// fmt.Printf("mismatch and cut at %dth, j: %d, start: %d\n", i, j, i-offset-1)
			rf.log = append(rf.log[:i-offset], entries[j:]...)
			// rf.log = append(copyLog(rf.log[:i-offset]), entries[j:]...)
			return false
		}
	}

	if esz > end {
		rf.log = append(rf.log, entries[end-start:]...)
	}

	return true
}

// TODO unit test me
// Find try to find last log wither within `term`
func (rf *Raft) findLogUpper(term int) *LogEntry {
	s, e := 0, len(rf.log)

	for s < e {
		m := (s + e) / 2
		if rf.log[m].Term > term {
			e = m
		} else { // rf.log[m] <= term
			s = m + 1
		}
	}

	// TODO unit test me for this case
	if s > 0 {
		s--
	}

	// if s == 0 {
	// 	DPrintf("incorrect upper of term %d, rf.log:", term)
	// 	for _, ent := range rf.log {
	// 		DPrintf("index: %d, term: %d\n", ent.Index, ent.Term)
	// 	}
	// 	panic("unreachable")
	// }
	return rf.log[s]
}

// rpc Implementation
// TODO bisect optimization
// TODO add case index mismatch, should reset election timer
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	role := rf.role
	reply.Term = rf.currentTerm
	reply.Success = false

	// stale request
	if rf.currentTerm > args.Term {
		DPrintf("append entries failed for follower %d because of stale term %d: %+v", rf.me, rf.currentTerm, *args)
		rf.mu.Unlock()
		return
	}

	needUpdate := false

	// update currentTerm and convert to follower
	// TODO if currentTerm == args.Term and role is candidate
	// we should convert it to follower
	// TODO add unit test to cover this situation
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		needUpdate = true
	}

	if rf.role != RoleFollower {
		rf.role = RoleFollower
		needUpdate = true
	}

	// handle stale entries, very tricky
	// TODO unit test me
	// delete conflict entries
	// ???????????? conflict ?????????????????????????????? entry, extreamly import
	offset := rf.log[0].Index
	prevLogIndex := args.PrevLogIndex
	// make sure that prevLogIndex is in the range of rf.log
	if prevLogIndex >= offset && prevLogIndex < len(rf.log)+offset {
		prevLogTerm := rf.log[prevLogIndex-offset].Term
		if prevLogTerm == args.PrevLogTerm {
			rf.appendLog(prevLogIndex, args.Entries)
			if len(args.Entries) > 0 {
				// TODO can be further optimized because appending log doesn't necessary change rf.log
				needUpdate = true
			}
			// DPrintf("follower %d append log from leader %d at term %d, prevLogIndex %d, entries: %+v, log after append: %+v", rf.me, args.LeaderID, rf.currentTerm, prevLogIndex, args.Entries, rf.log)
			DPrintf("follower %d append log from leader %d at term %d, prevLogIndex %d", rf.me, args.LeaderID, rf.currentTerm, prevLogIndex)
			if args.LeaderCommit > rf.commitIndex {
				lastIndex := rf.log[0].Index + len(rf.log)
				newCommitIndex := minInt(lastIndex, args.LeaderCommit)
				// bug on rf.commitIndex being trunked
				if newCommitIndex < rf.commitIndex {
					panic(fmt.Sprintf("%s %d commited entries trunked from %d to %d", rf.role, rf.me, rf.commitIndex, newCommitIndex))
				}

				if newCommitIndex > rf.commitIndex {
					rf.commitIndex = newCommitIndex
					rf.tryApplyLog()
				}
			}

			reply.Success = true
		} else {
			DPrintf("follower %d failed appending log beacause log term mismatch, log size %d, offset: %d, prevLogIndex %d", rf.me, len(rf.log)+offset, offset, prevLogIndex)
			if prevLogTerm < args.PrevLogTerm {
				reply.NextTryLogIndex = prevLogIndex
				reply.NextTryLogTerm = prevLogTerm
			} else { // prevLogTerm > args.PrevLogTerm
				// TODO handle log compaction
				ent := rf.findLogUpper(args.PrevLogTerm)
				reply.NextTryLogIndex = ent.Index
				reply.NextTryLogTerm = ent.Term
			}
		}
	} else if prevLogIndex >= offset+len(rf.log) { // out of range
		// fmt.Printf("server %d, testing leader prevLogIndex %d, prevLogTerm %d\n", rf.me, args.PrevLogIndex, args.PrevLogTerm)
		// fmt.Printf("server %d log: %+v\n", rf.me, rf.log)
		reply.NextTryLogIndex = len(rf.log) + rf.log[0].Index - 1
		reply.NextTryLogTerm = rf.log[len(rf.log)-1].Term
		DPrintf("follower %d failed appending log beacause log index mismatch, log size %d, prevLogIndex %d", rf.me, len(rf.log)+offset, prevLogIndex)
	} else {
		// prevLogIndex < offset
		// follower take snapshot?
		// TODO unit test me
		// panic("unreachable")
		reply.NextTryLogIndex = len(rf.log) + rf.log[0].Index - 1
		reply.NextTryLogTerm = rf.log[len(rf.log)-1].Term
	}

	if needUpdate {
		rf.persist(false)
	}

	rf.mu.Unlock()
	params := hbParams{
		appendEntriesArgs:  *args,
		appendEntriesReply: *reply,
	}

	select {
	case rf.hbChan <- params:
	case <-time.After(MinElectionTimeout * time.Millisecond):
		DPrintf("%s %d timeout sending heartbeat result at term %d", role, rf.me, reply.Term)
	}
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// fmt.Printf("server %d starting %+v", rf.me, command)
	rch := make(chan AppendResult, 1)
	bufEnt := LogEntryWrapper{
		command: command,
		retCh:   rch,
	}
	if !rf.logBuf.Append(bufEnt) {
		DPrintf("server %d append queue closed, return", rf.me)
		return -1, -1, false
	}

	DPrintf("server %d waiting command %+v to be appended", rf.me, command)
	ret := <-bufEnt.retCh
	DPrintf("server %d waiting command %+v appended finished", rf.me, command)
	return ret.index, ret.term, ret.isLeader
}

func (rf *Raft) appendQueueCallback(o interface{}) {
	DPrintf("server %d handling closed queue command %+v\n", rf.me, o)
	cmd := o.(LogEntryWrapper)
	cmd.retCh <- AppendResult{
		isLeader: false,
		index:    -1,
		term:     -1,
	}
	DPrintf("server %d done handling closed queue command %+v\n", rf.me, o)
}

func (rf *Raft) runLogAppender() {
	// fmt.Printf("server %d appender running\n", rf.me)
	for !rf.killed() {
		// fmt.Printf("server %d taking...\n", rf.me)
		cmds := rf.logBuf.TakeAll()
		// fmt.Printf("server %d taked...\n", rf.me)
		// fmt.Printf("server %d taked...\n", rf.me)
		if len(cmds) <= 0 {
			// fmt.Printf("empty log\n")
			continue
		}
		// fmt.Printf("server %d appender taking log %d\n", rf.me, len(cmds))
		// fmt.Printf("server %d appender taking log %d\n", rf.me, len(cmds))
		// fmt.Printf("server %d appending log %d at %d\n", rf.me, len(cmds), time.Now().UnixNano())

		// fmt.Printf("locking\n")
		rf.mu.Lock()
		// fmt.Printf("locked\n")
		index := rf.log[0].Index + len(rf.log)
		// fmt.Printf("logs: %+v, sz: %d, index: %d\n", rf.log, len(rf.log), rf.log[0].Index)
		isLeader := rf.role == RoleLeader
		for i := range cmds {
			e := cmds[i].(LogEntryWrapper)
			ret := AppendResult{
				isLeader: isLeader,
			}

			if isLeader {
				ret.index = index
				index++
				ret.term = rf.currentTerm
				ent := &LogEntry{
					Term:    ret.term,
					Index:   ret.index,
					Command: e.command,
				}
				rf.log = append(rf.log, ent)
			}
			e.retCh <- ret
		}

		// fmt.Printf("sending hb\n")
		if isLeader && len(cmds) > 0 {
			rf.persist(false)
			rf.nextIndex[rf.me] = index
			rf.matchIndex[rf.me] = index - 1

			go func() {
				select {
				case rf.appendLogChan <- struct{}{}:
				case <-time.After(64 * time.Millisecond): // FIXME magic number
				}
			}()
		}

		// fmt.Printf("unlocked\n")
		rf.mu.Unlock()
	}
}

func (rf *Raft) _Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	isLeader := rf.role == RoleLeader
	if !isLeader {
		rf.mu.Unlock()
		return -1, -1, false
	}

	index := rf.log[0].Index + len(rf.log)
	term := rf.currentTerm
	ent := &LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, ent)
	rf.persist(false)
	rf.nextIndex[rf.me] = index + 1
	rf.matchIndex[rf.me] = index
	lastHB := rf.lastHB
	rf.mu.Unlock()

	go func() {
		time.Sleep(1 * time.Millisecond)
		rf.mu.Lock()
		latestHb := rf.lastHB
		rf.mu.Unlock()

		// we've got sent
		if lastHB != latestHb {
			return
		}

		select {
		case rf.appendLogChan <- struct{}{}:
			DPrintf("leader %d tirgger log appending", rf.me)
		case <-time.After(LeaderIdlePeriod):
			DPrintf("leader %d timeout triggering log appending", rf.me)
		}
	}()

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
	// TODO we don't need to close applyCh?
	rf.mu.Lock()
	defer rf.mu.Unlock()
	close(rf.applyCh)
	rf.logBuf.Close()
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
			rf.tryApplyLog()
			rf.mu.Unlock()
			eto := getElectionTimeout()
			rf.runFollower(term, eto)

		case RoleCandidate:
			start := time.Now()
			eto := getElectionTimeout()
			rf.votedFor = rf.me + 1
			rf.currentTerm++
			rf.persist(false)
			lastLogEnt := rf.log[len(rf.log)-1]
			lastLogIndex := lastLogEnt.Index
			lastLogTerm := lastLogEnt.Term
			rf.mu.Unlock()
			term++
			voteChan := make(chan struct{}, 1)
			go rf.startElection(term, voteChan, lastLogIndex, lastLogTerm)
			rf.runCandidate(term, start, eto, voteChan)

		case RoleLeader:
			start := time.Now()
			to := LeaderIdlePeriod
			argsList := rf.getHBEntries()
			rf.tryApplyLog()
			rf.lastHB = time.Now()
			rf.mu.Unlock()
			sendHBChan := make(chan hbParams, 1)
			go rf.sendHeartbeat(term, sendHBChan, argsList, false)
			rf.runLeader(term, start, to, sendHBChan)

		default:
			panic(fmt.Sprintf("unknown raft role: %s", rf.role))
		}
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(rand.Intn(ElectionTimeoutInterval)+MinElectionTimeout) * time.Millisecond
	// return MinElectionTimeout * time.Millisecond
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
		// TODO add new unit test:
		// a follower will convert to candidate despite its term has been
		// updated
		rf.mu.Lock()
		// try get hb or rv msg if any
		// TODO put me in a loop?
		// TODO add unit test: timeout, but there are pending hv or rv msg, we should not convert to follower
		select {
		case params := <-rf.hbChan:
			// there are blocking hb msg
			if params.appendEntriesArgs.Term >= term {
				rf.mu.Unlock()
				return
			}
		case params := <-rf.rvChan:
			// there are blocking rv msg, and we have grant vote to it
			if params.requestVoteArgs.Term >= term && params.requestVoteReply.VoteGranted {
				rf.mu.Unlock()
				return
			}
		case <-time.After(4 * time.Millisecond): // TODO add constant
			DPrintf("follower %d term changed from %d to %d but recieve no hb or rv msg", rf.me, term, rf.currentTerm)
		}

		// we don't care about the term and role now
		rf.role = RoleCandidate
		rf.mu.Unlock()

	case params := <-rf.hbChan:
		// stale hb msg
		if params.appendEntriesArgs.Term < term {
			DPrintf("follower %d recieve stale hb msg at term %d: %+v", rf.me, term, params)
			updateTO(&eto, start)
			goto WAIT
		}
		DPrintf("follower %d recieve hb msg from leader %d at term %d and return\n", rf.me, params.appendEntriesArgs.LeaderID, term)
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
	InstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool
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

func (r defaultRaftRPCManager) InstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return r.rf.sendInstallSnapshot(server, args, reply)
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
			DPrintf("candidate %d recieve vote result from %d: %+v at term %d", rf.me, server, *reply, term)

			if !reply.VoteGranted {
				DPrintf("candidate %d failed to get vote from %d at term %d", rf.me, server, term)
				if reply.Term <= term {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > term { // TODO do we need this?
					// double check on currentTerm, beacause it might have been changed by other rpc
					if reply.Term > rf.currentTerm {
						DPrintf("candidate %d convert to follower and update term from %d to %d", rf.me, rf.currentTerm, reply.Term)
						rf.currentTerm = reply.Term
						rf.role = RoleFollower
						rf.votedFor = 0
						rf.persist(false)
					}
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

				case <-time.After(MinElectionTimeout * time.Millisecond):
					DPrintf("candidate %d timeout sending successful election result at term %d", rf.me, term)
				}
			}
		}(i, args, reply)
	}
}

// TODO unit test me
func (rf *Raft) runCandidate(term int, start time.Time, eto time.Duration, voteChan chan struct{}) {
	// start := time.Now()
	updateTO(&eto, start)

WAIT:
	select {
	case <-time.After(eto):
		// timeout without result, begin another election
	case <-voteChan:
		// else we are leader now
		rf.mu.Lock()
		if rf.currentTerm == term && rf.role == RoleCandidate {
			DPrintf("candidate %d become leader at term %d", rf.me, rf.currentTerm)
			rf.role = RoleLeader
			// reinit nextIndex and matchIndex
			nextIndex := rf.log[0].Index + len(rf.log)
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = nextIndex
				if i != rf.me {
					rf.matchIndex[i] = 0
				} else {
					rf.matchIndex[i] = nextIndex - 1
				}
			}

			argList := rf.getHBEntries()
			// TODO consider commit empty log if commitLogIndex lag behind lastLogIndex
			// send empty heartbeat right away
			rf.lastHB = time.Now()
			rf.mu.Unlock()
			sendHBChan := make(chan hbParams, 1)
			rf.sendHeartbeat(term, sendHBChan, argList, false)
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

// TODO remove redundant sendEmptyHB parameter
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
		// TODO extract and unit test me, important
		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
			// DPrintf("leader %d sending entries %+v to server %d at term %d, send empty: %t\n", rf.me, *args, server, term, sendEmptyHB)
			DPrintf("leader %d sending entries %+v to server %d at term %d, send empty: %t\n", rf.me, nil, server, term, sendEmptyHB)
			ok := rf.rpcManager.SendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			// if rf.currentTerm != term { // rf.currentTerm should have been updated
			// 	rf.mu.Unlock()
			// 	params := hbParams{
			// 		appendEntriesReply: *reply,
			// 	}

			// 	select {
			// 	case sendHBChan <- params:
			// 	case <-time.After(MinElectionTimeout * time.Millisecond):
			// 		DPrintf("leader %d timeout sending role update msg from %d at term %d, new term is %d", rf.me, server, term, reply.Term)
			// 	}

			// 	return
			// }
			// rf.currentTerm == term
			if term < reply.Term { // TODO do we really need this?
				// TODO unit test me
				// rf.currentTerm might have been updated if we set rf.voteFor = 0
				// or rf.currentTerm = reply.Term directly we might override exiting value
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.role = RoleFollower
					if rf.votedFor == rf.me+1 {
						rf.votedFor = 0
					}

					rf.persist(false)
				}

				rf.mu.Unlock()
				params := hbParams{
					appendEntriesReply: *reply,
				}

				select {
				case sendHBChan <- params:
				case <-time.After(MinElectionTimeout * time.Millisecond):
					DPrintf("leader %d timeout sending role update msg from %d at term %d, new term is %d", rf.me, server, term, reply.Term)
				}

				return
			}

			// role changed return directly
			// TODO add unit test: role changed, and leader's log might got truncated
			// if we proceed to append log, reply.NextTryLogIndex might exceed rf.log
			if rf.currentTerm != term || rf.role != RoleLeader {
				rf.mu.Unlock()
				return
			}

			// if sendEmptyHB {
			// 	rf.mu.Unlock()
			// 	return
			// }

			// term >= reply.Term
			if reply.Success {
				// if rf.nextIndex[server] != args.PrevLogIndex+1, that means
				// this is a stale result and we should ignore
				if rf.nextIndex[server] == args.PrevLogIndex+1 {
					rf.nextIndex[server] += len(args.Entries)
					// should avoid calling tryUpdateCommitIndex() too often!
					// if rf.matchIndex[server] == rf.nextIndex[server]-1 that means
					// nothing changed so we don't need perform commitIndex update
					if rf.matchIndex[server] != rf.nextIndex[server]-1 {
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						rf.tryUpdateCommitIndex()
					}
				}

				rf.mu.Unlock()
			} else {
				// log unmatch, decrease by one
				// TODO optimize index matching algorithm
				if rf.nextIndex[server] == args.PrevLogIndex+1 {
					// are we within legal range
					offset := rf.log[0].Index
					if reply.NextTryLogIndex >= offset && reply.NextTryLogIndex < offset+len(rf.log) {
						nextTryLogTerm := rf.log[reply.NextTryLogIndex-offset].Term
						if reply.NextTryLogTerm == nextTryLogTerm {
							rf.nextIndex[server] = reply.NextTryLogIndex + 1
							// fmt.Printf("leader log: %+v\n", rf.log)
						} else if reply.NextTryLogTerm > nextTryLogTerm {
							rf.nextIndex[server] = reply.NextTryLogIndex
							if reply.NextTryLogIndex == offset { // commited log entry must match
								panic("unreachable")
							}
							// fmt.Printf("server %d next index of term %d is %d\n", server, reply.NextTryLogTerm, rf.nextIndex[server])
						} else { // reply.NextTryLogTerm < nextTryLogTerm
							ent := rf.findLogUpper(reply.NextTryLogTerm)
							if rf.nextIndex[server] != ent.Index+1 {
								rf.nextIndex[server] = ent.Index + 1
							}

							// TODO unit test me, if term mismatch, we should further test
							// whether we reach the very begining of rf.log
							if ent.Term != reply.NextTryLogTerm && ent.Index == rf.log[0].Index {
								DPrintf("leader %d sending snapshot, because, ent: %+v, and reply: %+v", rf.me, *ent, *reply)
								// FIXME why ent.Term != reply.NextTryLogTerm in TestFigure82C
								// when we are not enabling log compaction? One situation is that
								// log entry of term reply.NextTryLogTerm and rf.NextTryLogIndex
								// is discarded, eg:
								// index: 			1 2 3
								// leader term:     3 5 5
								// follower term:   3 3 4
								// follower.log[3] is discarded so ent.Term != reply.NextTryLogTerm
								rf.sendSnapshot(server)
							}
							// TODO might need to install snapshot here
							// fmt.Printf("server %d find upper of %d: %+v\n", server, reply.NextTryLogTerm, ent)
						}
						// fmt.Printf("server %d nextIndex %d\n", server, rf.nextIndex[server])
					} else if reply.NextTryLogIndex < offset {
						// follower log lag behind, install snapshot
						// TODO avoid too many calls
						// TODO check why 2c trigger sendSnapshot() important
						rf.sendSnapshot(server)
						// snapshot := make([]byte, len(rf.snapshot))
						// copy(snapshot, rf.snapshot)
						// snapshotArgs := &InstallSnapshotArgs{
						// 	Term:              rf.currentTerm,
						// 	LeaderID:          rf.me,
						// 	LastIncludedIndex: rf.log[0].Index,
						// 	LastIncludedTerm:  rf.log[0].Term,
						// 	Data:              snapshot,
						// }
						// snapshotReply := &InstallSnapshotReply{}
						// go func(snapshotArgs *InstallSnapshotArgs, snapshotReply *InstallSnapshotReply, term int) {
						// 	if rf.sendInstallSnapshot(server, snapshotArgs, snapshotReply) {
						// 		DPrintf("leader %d failed calling sendInstallSnapshot at term %d", rf.me, term)
						// 		return
						// 	}

						// 	// TODO check reply term and persist if necessary
						// }(snapshotArgs, snapshotReply, rf.currentTerm)
						// panic("no implemented")
					} else { // reply.NextTryLogIndex >= offset + len(rf.log)
						panic("unreachable")
					}

					// rf.nextIndex[server]--
				} else {
					DPrintf("leader %d failed to update nextIndex of %d beacause of mismatch, rf.nextIndex[server] %d, prevLogIndex %d", rf.me, server, rf.nextIndex[server], args.PrevLogIndex)
				}
				rf.mu.Unlock()
			}
		}(i, args, reply)
	}
}

// assume rf.mu lock hold
func (rf *Raft) sendSnapshot(server int) {
	snapshot := make([]byte, len(rf.snapshot))
	copy(snapshot, rf.snapshot)
	snapshotArgs := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.log[0].Index,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              snapshot,
	}
	snapshotReply := &InstallSnapshotReply{}
	go func(snapshotArgs *InstallSnapshotArgs, snapshotReply *InstallSnapshotReply, term int) {
		if !rf.sendInstallSnapshot(server, snapshotArgs, snapshotReply) {
			DPrintf("leader %d failed calling sendInstallSnapshot at term %d", rf.me, term)
			return
		}

		// TODO check reply term and persist if necessary
	}(snapshotArgs, snapshotReply, rf.currentTerm)
}

// assume rf.mu lock hold
// TODO add ut: raft should gurrantee that commited entries are applied asap
func (rf *Raft) tryApplyLog() {
	DPrintf("server %d try applying log, commitIndex %d, lastApplied %d", rf.me, rf.commitIndex, rf.lastApplied)
	if rf.commitIndex > rf.lastApplied && !rf.killed() {
		offset := rf.log[0].Index
		for rf.lastApplied < rf.commitIndex {
			index := rf.lastApplied + 1
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: index,
				Command:      rf.log[index-offset].Command,
			}
			DPrintf("server %d applying msg %+v at term %d", rf.me, msg, rf.currentTerm)
			// msg.Command = rf.log[index-offset].Command
			select {
			case rf.applyCh <- msg:
				rf.lastApplied++

			// TODO unit test me, hold global lock and too long will block
			// other goroutines and lead to disastrous result, for example
			// that might starve an rf.Start() operation and that might in
			// turn stuck kvserver's command execution.
			// case <-time.After(LeaderIdlePeriod):
			case <-time.After(4 * time.Millisecond):
				// TODO commit msg if lastApplied < rf.commitIndex
				DPrintf("leader %d timeout commiting msg %d at term %d", rf.me, rf.lastApplied, rf.currentTerm)
				return
			}
		}
		DPrintf("server %d update commit index to %d at term %d, last applied: %d", rf.me, rf.commitIndex, rf.currentTerm, rf.lastApplied)
	}
}

// call by leader
// assume rf.mu lock hold
// TODO unit test me
func (rf *Raft) tryUpdateCommitIndex() {
	DPrintf("server %d try updating commit index", rf.me)
	sz := len(rf.matchIndex)
	matchIndex := make([]int, sz)
	copy(matchIndex, rf.matchIndex)
	sort.Slice(matchIndex, func(i, j int) bool {
		return matchIndex[i] < matchIndex[j]
	})

	newCommitIndex := matchIndex[sz/2]
	offset := rf.log[0].Index
	// be careful of underflow, the computed newCommitIndex might lag far behind offset
	if newCommitIndex <= offset {
		DPrintf("server %d newCommitIndex %d, offset %d", rf.me, newCommitIndex, offset)
		return
	}

	ent := rf.log[newCommitIndex-offset]
	// leader only commit log from current term
	if ent.Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
		rf.commitIndex = newCommitIndex
		rf.tryApplyLog()
	}
	DPrintf("server %d finish updating commit index", rf.me)
}

func (rf *Raft) runLeader(term int, start time.Time, to time.Duration, sendHBChan chan hbParams) {
	updateTO(&to, start)
	// send heartbeat
	// go rf.sendHeartbeat(term)

WAIT:
	select {
	case <-time.After(to):
		// now send heart beat again

	case <-rf.appendLogChan:
		// TODO rate limit
		// append log, right now!

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
		nullEnt := &LogEntry{
			Index: 0,
			Term:  0,
		}
		rf.log = append(rf.log, nullEnt)
		rf.persist(false)
	}

	rf.snapshot = persister.ReadSnapshot()
	rf.role = RoleFollower
	rf.hbChan = make(chan hbParams, 1)
	rf.rvChan = make(chan rvParams, 1)
	rf.appendLogChan = make(chan struct{}, 1)
	rf.applyCh = applyCh
	rf.lastApplied = rf.log[0].Index
	rf.commitIndex = rf.log[0].Index
	rf.logBuf = newQueue(rf.appendQueueCallback)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.rpcManager = &defaultRaftRPCManager{rf}

	// start ticker goroutine to start elections
	go rf.runLogAppender()
	go rf.ticker()

	return rf
}
