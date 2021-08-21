package raft

import (
	"sync/atomic"
	"testing"
	"time"

	"6.824/labrpc"
)

type fakeRaftRPCManager struct {
	rf                    *Raft
	replyTerm             int
	replySuccess          bool
	appendEntriesRPCCount int32
	requestVoteRPCCount   int32
}

func (m *fakeRaftRPCManager) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	atomic.AddInt32(&m.requestVoteRPCCount, 1)
	return false
}

func (m *fakeRaftRPCManager) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	atomic.AddInt32(&m.appendEntriesRPCCount, 1)
	// TODO implement me
	// fmt.Printf("after add %d, %p\n", atomic.LoadInt32(&m.appendEntriesRPCCount), &m.appendEntriesRPCCount)
	// fmt.Printf("after add %d\n", m.appendEntriesRPCCount)
	// fmt.Printf("recieve append entries call\n")
	reply.Success = m.replySuccess
	reply.Term = m.replyTerm
	return true
}

func newFakeRaftRPCManager(replyTerm int, replySuccess bool) *fakeRaftRPCManager {
	return &fakeRaftRPCManager{
		replyTerm:    replyTerm,
		replySuccess: replySuccess,
	}
}

func newTestRaft(numPeers int, term int, role RaftRole) *Raft {
	rf := &Raft{}
	rf.peers = make([]*labrpc.ClientEnd, numPeers)
	rf.me = 0
	rf.role = role
	rf.currentTerm = term

	// start ticker goroutine to start elections
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	// rf.raftRPCManager = &fakeRaftRPCManager{rf: rf}

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

func TestUTAppendEntries(t *testing.T) {
	// cases:
	//  1. ~~ignore stale request~~
	//  2. ~~follower handle normal request(should resset election timer)~~
	//  3. ~~candidate handle normal request, update term and convert to follower~~
	cases := []struct {
		name              string
		numPeers          int
		currentTerm       int
		expectedRaftTerm  int
		expectedReplyTerm int
		expectedSuccess   bool
		expectedRaftRole  RaftRole
		role              RaftRole
		args              *AppendEntriesArgs
		reply             *AppendEntriesReply
		bolck             bool
	}{
		{
			name:              "Ignore stale append entries",
			numPeers:          3,
			currentTerm:       1,
			expectedRaftTerm:  1,
			expectedReplyTerm: 1,
			expectedRaftRole:  RoleFollower,
			role:              RoleFollower,
			expectedSuccess:   false,
			args: &AppendEntriesArgs{
				Term:     0,
				LeaderID: 1,
			},
			reply: &AppendEntriesReply{},
			bolck: true,
		},
		{
			name:              "Normal hb request",
			numPeers:          3,
			currentTerm:       1,
			role:              RoleFollower,
			expectedReplyTerm: 1,
			expectedRaftTerm:  1,
			expectedRaftRole:  RoleFollower,
			args: &AppendEntriesArgs{
				Term:     1,
				LeaderID: 1,
			},
			expectedSuccess: true,
			reply:           &AppendEntriesReply{},
			bolck:           false,
		},
		{
			name:              "Candidate handle normal request, update term and convert to follower",
			numPeers:          3,
			currentTerm:       0,
			role:              RoleCandidate,
			expectedReplyTerm: 0,
			expectedRaftTerm:  1,
			expectedRaftRole:  RoleFollower,
			args: &AppendEntriesArgs{
				Term:     1,
				LeaderID: 1,
			},
			expectedSuccess: true,
			reply:           &AppendEntriesReply{},
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.numPeers, c.currentTerm, c.role)
		rf.AppendEntries(c.args, c.reply)

		if c.reply.Success != c.expectedSuccess {
			t.Errorf("Error running %s, expect reply.success %t but got %t", c.name, c.expectedSuccess, c.reply.Success)
		}

		if c.reply.Term != c.expectedReplyTerm {
			t.Errorf("Error running %s, expect reply.Term %d but got %d", c.name, c.expectedReplyTerm, c.reply.Term)
		}

		if c.expectedRaftRole != rf.role {
			t.Errorf("Error running %s, expect raft.role %s but got %s", c.name, c.expectedRaftRole, rf.role)
		}

		if rf.currentTerm != c.expectedRaftTerm {
			t.Errorf("Error running %s, expect raft.currentTerm %d but got %d", c.name, c.expectedRaftTerm, rf.currentTerm)
		}
	}

	// more to do
	// TODO handle log entry appending
}

func TestUTRequestVote(t *testing.T) {
	// TODO
	// cases:
	// 1. vote granted
	// 	1.1 stale term
	// 	1.2 vote granted to other candidate
	// 2. vote no granted
	cases := []struct {
		name     string
		numPeers int
		term     int
		role     RaftRole
	}{
		{
			name:     "request vote, stale term",
			numPeers: 3,
			term:     0,
			role:     RoleCandidate,
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.numPeers, c.term, c.role)
		t.Log(rf)
		// TODO
		// rf.RequestVote(l)
	}
}

func TestStartElection(t *testing.T) {
	// TODO
	// cases:
	//  1. win an election
	//  2. lost an election
	//  3. election tie
	//  4. recieve higher term and convert to follower
}

func TestUTSendHeartbeat(t *testing.T) {
	// cases:
	//  1. recieve same term and remain follower
	//  2. recieve higher term and convert to follower
	cases := []struct {
		peerCount    int
		isLeader     bool
		replySuccess bool
		replyTerm    int
		currentTerm  int
		expectedTerm int
	}{
		{
			peerCount:    3,
			isLeader:     true,
			replyTerm:    0,
			currentTerm:  0,
			replySuccess: true,
			expectedTerm: 0,
		},
		{
			peerCount:    3,
			isLeader:     false,
			replyTerm:    1,
			currentTerm:  0,
			replySuccess: false,
			expectedTerm: 1,
		},
	}

	for i, c := range cases {
		rf := newTestRaft(c.peerCount, c.currentTerm, RoleLeader)
		rpcManager := newFakeRaftRPCManager(c.replyTerm, c.replySuccess)
		rf.rpcManager = rpcManager

		rf.sendHeartbeat(rf.currentTerm)
		// wait for rpc being called
		time.Sleep(MaxElectionTimeout * time.Millisecond)
		rpcCount := int(atomic.LoadInt32(&(rpcManager.appendEntriesRPCCount)))
		if rpcCount != c.peerCount-1 {
			t.Errorf("case %d expect %d append entries rpc but got %d\n", i+1, c.peerCount-1, rpcCount)
		}

		term, isLeader := rf.GetState()
		if term != c.expectedTerm {
			t.Errorf("case %d expect raft to at term %d but got %d", i, c.expectedTerm, term)
		}

		if isLeader != c.isLeader {
			t.Errorf("case %d expect raft to be leader %t, but got %t", i, c.isLeader, isLeader)
		}
	}
}
