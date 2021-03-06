package raft

import (
	"fmt"
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	// offset            int
	// done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		DPrintf("server %d killed at term %d, return from install snapshot", rf.me, rf.currentTerm)
		return
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	needUpdate := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		needUpdate = true
	}

	if rf.role != RoleFollower {
		rf.role = RoleFollower
		needUpdate = true
	}

	if needUpdate {
		rf.persist(false)
	}

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	select {
	// TODO check applyCh before send msg
	case rf.applyCh <- msg:
		DPrintf("server %d sent snapshot at term %d from leader %d, server shoud invoke CondInstallSnapshot() to finish the update", rf.me, rf.currentTerm, args.LeaderID)
	case <-time.After(MaxElectionTimeout * time.Millisecond):
		DPrintf("server %d timeout sending snapshot msg at term %d from leader %d", rf.me, rf.currentTerm, args.LeaderID)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.log[0].Term
	index := rf.log[0].Index
	if lastIncludedTerm < term ||
		// TODO unit test me, very important, god damn important,
		// install a snapshot with lastIncludeIndex < rf.lastApplied is disastrous
		(lastIncludedIndex < rf.lastApplied) ||
		(lastIncludedTerm == term && lastIncludedIndex <= index) {
		DPrintf("server %d failed install snapshot, lastIncludedTerm %d, lastIncludedIndex %d, term %d, index %d, lastApplied %d", rf.me, lastIncludedTerm, lastIncludedIndex, term, index, rf.lastApplied)
		return false
	}

	// trim log
	offset := index
	pos := lastIncludedIndex - offset
	if pos < 0 {
		panic(fmt.Sprintf("server %d imposible lastIncludedTerm %d, lastIncludedIndex %d, log: %+v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.log))
	}

	DPrintf("server %d installing snapshot at term %d, lastIncludedIndex %d, lastIncludedTerm %d, lastApplied before update %d, commitIndex before update %d", rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.lastApplied, rf.commitIndex)
	if pos >= len(rf.log) {
		rf.log = []*LogEntry{
			{
				Index: lastIncludedIndex,
				Term:  lastIncludedTerm,
			},
		}
	} else {
		// TODO unit test me: don't forget to override rf.log[0] in this case
		// rf.log = rf.log[pos:]
		rf.log = copyLog(rf.log[pos:])
		// override term of rf.log[0]
		if rf.log[0].Index != lastIncludedIndex {
			DPrintf("server %d fatal, log offset do not match leader, offset %d and term %d, leader index %d and term %d", rf.me, rf.log[0].Index, rf.log[0].Term, lastIncludedIndex, lastIncludedTerm)
			rf.log[0].Index = lastIncludedIndex
			// truncate all entries after rf.log[0]
			rf.log = rf.log[:1]
		}

		if rf.log[0].Term != lastIncludedTerm {
			rf.log[0].Term = lastIncludedTerm
			// truncate all entries after rf.log[0]
			rf.log = rf.log[:1]
		}
	}

	// update raft state and snapshot
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.snapshot = snapshot
	rf.persist(true)

	return true
}

func copyLog(ents []*LogEntry) []*LogEntry {
	sz := len(ents)
	replica := make([]*LogEntry, sz)
	copy(replica, ents)

	return replica
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	offset := rf.log[0].Index
	pos := index - offset
	if pos < 0 {
		DPrintf("server %d underflow taking snapshot at %d at term %d", rf.me, index, rf.currentTerm)
		return
	}

	if pos >= len(rf.log) {
		DPrintf("server %d overflow taking snapshot at %d at term %d", rf.me, index, rf.currentTerm)
		return
	}

	rf.snapshot = snapshot
	// rf.log = rf.log[pos:]
	rf.log = copyLog(rf.log[pos:])

	// persist raft state with snapshot
	rf.persist(true)
}
