package raft

import (
	"testing"

	"6.824/labrpc"
)

type fakeRaftRPCManager struct {
}

func (m *fakeRaftRPCManager) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return false
}

func (m *fakeRaftRPCManager) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return false
}

func newFakeRaftRPCManager() RaftRPCManager {
	return &fakeRaftRPCManager{}
}

func NewTestRaft(numPeers int) *Raft {
	rf := &Raft{}
	rf.peers = make([]*labrpc.ClientEnd, numPeers, numPeers)
	rf.me = 0

	// start ticker goroutine to start elections
	rf.role = RoleFollower
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	rf.raftRPCManager = &defaultRaftRPCManager{rf}

	return rf
}

func TestRunFollower(t *testing.T) {
	// TODO
	// cases:
	// 1. eto time out, become candidate
	// 2. receive hb msg from leader
	// 3. receive stale hb msg
	// 4. receive rv msg and grant vote
	// 5. receive rv msg but not vote granted
	// 6. receive stale rv msg
}

func TestRunCandidate(t *testing.T) {
	// TODO
	// cases:
	// 1. eto time out, vote again
	// 2. win vote, become leader
	// 3. recieve stale hb msg
	// 4. recieve hb msge from leader
	// 5. recive stale rv msg
	// 6. recieve rv msg but not vote granted
	// 7. recieve rv msg and grant vote
}

func TestStartElection(t *testing.T) {
	// TODO
	// cases:
	//  1. win an election
	//  2. lost an election
	//  3. election tie
	//  4. recieve higher term and convert to follower
}

func TestSendHeartbeat(t *testing.T) {
	// TODO
	// cases:
	//  1. recieve higher term and convert to follower
}

func TestRunLeader(t *testing.T) {
	// TODO
	// cases:
	// 1. hbto time out, send hb again
	// 2. recieve stale hb msg
	// 3. recieve hb msg from leader
	// 4. recieve stale rv msg
	// 5. recieve rv msg but not vote granted
	// 6. recieve rv msg and grant vote
}

func TestAppendEntries(t *testing.T) {
	// TODO
}

func TestRequestVote(t *testing.T) {
	// TODO
}
