package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"6.824/labrpc"
)

type raftStatus struct {
	numPeers int
	term     int
	votedFor int
	role     RaftRole
}

type testAppendEntriesReply struct {
	ok    bool
	reply *AppendEntriesReply
}

type testRequestVoteReply struct {
	ok    bool
	reply *RequestVoteReply
}

type fakeRaftRPCManager struct {
	// rf                    *Raft
	appendEntriesRPCCount int32
	requestVoteRPCCount   int32
	appendLock            sync.Mutex
	appendEntriesReplies  []*testAppendEntriesReply
	appendEntriesIndex    int
	voteLock              sync.Mutex
	requestVoteReplies    []*testRequestVoteReply
	requestVoteIndex      int
}

func (m *fakeRaftRPCManager) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	atomic.AddInt32(&m.requestVoteRPCCount, 1)
	m.voteLock.Lock()
	defer m.voteLock.Unlock()
	sz := len(m.requestVoteReplies)
	if m.requestVoteIndex >= sz {
		panic(fmt.Sprintf("request vote index %d out of range, max length %d", m.requestVoteIndex, sz))
	}

	testReply := m.requestVoteReplies[m.requestVoteIndex]
	*reply = *testReply.reply
	m.requestVoteIndex = (m.requestVoteIndex + 1) % sz
	// fmt.Printf("respond to vote: %+v\n", *reply)

	return testReply.ok
}

func (m *fakeRaftRPCManager) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	atomic.AddInt32(&m.appendEntriesRPCCount, 1)
	m.appendLock.Lock()
	defer m.appendLock.Unlock()
	sz := len(m.appendEntriesReplies)
	if m.appendEntriesIndex >= sz {
		panic(fmt.Sprintf("append entries index %d out of range, max length %d", m.appendEntriesIndex, sz))
	}

	testReply := m.appendEntriesReplies[m.appendEntriesIndex]
	*reply = *testReply.reply
	m.appendEntriesIndex = (m.appendEntriesIndex + 1) % sz

	return testReply.ok
}

func newFakeRaftRPCManager(appendReplies []*testAppendEntriesReply, voteReplies []*testRequestVoteReply) *fakeRaftRPCManager {
	return &fakeRaftRPCManager{
		appendEntriesReplies: appendReplies,
		requestVoteReplies:   voteReplies,
	}
}

func _newTestRaft(status *raftStatus) *Raft {
	rf := &Raft{}
	rf.peers = make([]*labrpc.ClientEnd, status.numPeers)
	rf.me = 0
	rf.role = status.role
	rf.currentTerm = status.term
	rf.votedFor = status.votedFor

	// start ticker goroutine to start elections
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	// rf.raftRPCManager = &fakeRaftRPCManager{rf: rf}

	return rf

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
	// cases:
	// 1. vote no granted
	// 	1.1 stale term
	// 	1.2 vote granted to other candidate
	// 1. vote granted
	// what to check
	//  1. reply term
	//  2. reply success status
	//  3. raft term
	//  4. raft role
	cases := []struct {
		name                     string
		rfStatus                 *raftStatus
		numPeers                 int
		term                     int
		role                     RaftRole
		args                     *RequestVoteArgs
		reply                    *RequestVoteReply
		expectedReplyTerm        int
		expectedReplyVoteGranted bool
		expectedRaftTerm         int
		expectedRaftRole         RaftRole
	}{
		{
			name: "request vote, stale term",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     2,
				role:     RoleLeader,
				votedFor: 0 + 1, // voted for self
			},
			args: &RequestVoteArgs{
				CandiateID:   1,
				Term:         1,
				LastLogTerm:  0, // TODO update for log stale check
				LastLogIndex: 0,
			},
			reply:                    &RequestVoteReply{},
			expectedReplyTerm:        2,
			expectedReplyVoteGranted: false,
			expectedRaftTerm:         2,
			expectedRaftRole:         RoleLeader,
		},
		{
			name: "request vote, vote granted to others",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     2,
				role:     RoleLeader,
				votedFor: 1, // voted for self
			},
			args: &RequestVoteArgs{
				CandiateID:   1,
				Term:         2,
				LastLogTerm:  0, // TODO update for log stale check
				LastLogIndex: 0,
			},
			reply:                    &RequestVoteReply{},
			expectedReplyTerm:        2,
			expectedReplyVoteGranted: false,
			expectedRaftTerm:         2,
			expectedRaftRole:         RoleLeader,
		},
		{
			name: "vote granted",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     2,
				role:     RoleLeader,
				votedFor: 1, // voted for self
			},
			args: &RequestVoteArgs{
				CandiateID:   1,
				Term:         3,
				LastLogTerm:  0, // TODO update for log stale check
				LastLogIndex: 0,
			},
			reply:                    &RequestVoteReply{},
			expectedReplyTerm:        2,
			expectedReplyVoteGranted: true,
			expectedRaftTerm:         3,
			expectedRaftRole:         RoleFollower,
		},
	}

	for _, c := range cases {
		rf := _newTestRaft(c.rfStatus)
		rf.RequestVote(c.args, c.reply)

		if c.reply.Term != c.expectedReplyTerm {
			t.Errorf("Error running %s: expect reply term %d but got %d", c.name, c.expectedRaftTerm, c.reply.Term)
		}

		if c.reply.VoteGranted != c.expectedReplyVoteGranted {
			t.Errorf("Error running %s: expect reply voteGranted %t but got %t", c.name, c.expectedReplyVoteGranted, c.reply.VoteGranted)
		}

		if c.expectedRaftTerm != rf.currentTerm {
			t.Errorf("Error running %s: expect raft term %d but got %d", c.name, c.expectedRaftTerm, rf.currentTerm)
		}

		if c.expectedRaftRole != rf.role {
			t.Errorf("Error running %s: expect raft role %s but got %s", c.name, c.expectedRaftRole, rf.role)
		}
	}
}

func TestUTStartElection(t *testing.T) {
	// TODO
	// cases:
	//  1. win an election
	//  2. election tie
	//  3. lost an election(recieve higher term and convert to follower)
	cases := []struct {
		name        string
		peerCount   int
		currentTerm int
		voteReplies []*testRequestVoteReply
		// expectedRole RaftRole
		// expectedTerm int
		win bool
	}{
		{
			name:        "win election",
			peerCount:   3,
			currentTerm: 0,
			voteReplies: []*testRequestVoteReply{
				{
					ok: true,
					reply: &RequestVoteReply{
						Term:        0,
						VoteGranted: true,
					},
				},
			},
			// expectedRole: RoleLeader,
			// expectedTerm: 0,
			win: true,
		},
		{
			name:        "election tie",
			peerCount:   3,
			currentTerm: 0,
			voteReplies: []*testRequestVoteReply{
				{
					ok: true,
					reply: &RequestVoteReply{
						Term:        0,
						VoteGranted: false,
					},
				},
			},
			win: false,
		},
		{
			name:        "lose an election",
			peerCount:   3,
			currentTerm: 0,
			voteReplies: []*testRequestVoteReply{
				{
					ok: true,
					reply: &RequestVoteReply{
						Term:        1,
						VoteGranted: false,
					},
				},
			},
			win: false,
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.peerCount, c.currentTerm, RoleCandidate)
		rpcManager := newFakeRaftRPCManager(nil, c.voteReplies)
		rf.rpcManager = rpcManager

		resultChan := make(chan struct{})
		rf.startElection(rf.currentTerm, resultChan)
		// time.Sleep(10 * MaxElectionTimeout * time.Millisecond)

		select {
		case <-resultChan:
			if !c.win {
				t.Errorf("case %s expect not to win an election but not", c.name)
			}
		case <-time.After(MaxElectionTimeout * time.Millisecond):
			if c.win {
				t.Errorf("case %s expect to win an election but not", c.name)
			}
		}

		// if rf.role != c.expectedRole {
		// 	t.Errorf("case %s expect raft to be role %s but got %s", c.name, c.expectedRole, rf.role)
		// }

		// if rf.currentTerm != c.expectedTerm {
		// 	t.Errorf("case %s expect raft to at term %d but got %d", c.name, c.expectedTerm, rf.currentTerm)
		// }
	}
}

func TestUTSendHeartbeat(t *testing.T) {
	// cases:
	//  1. recieve same term and remain leader
	//  2. recieve higher term and convert to follower
	cases := []struct {
		name          string
		peerCount     int
		isLeader      bool
		currentTerm   int
		expectedTerm  int
		appendReplies []*testAppendEntriesReply
	}{
		{
			name:         "'remain leader'",
			peerCount:    3,
			isLeader:     true,
			currentTerm:  0,
			expectedTerm: 0,

			appendReplies: []*testAppendEntriesReply{
				{
					ok: true,
					reply: &AppendEntriesReply{
						Success: true,
						Term:    0,
					},
				},
			},
		},
		{
			name:         "'converted to follower'",
			peerCount:    3,
			isLeader:     false,
			currentTerm:  0,
			expectedTerm: 1,
			appendReplies: []*testAppendEntriesReply{
				{
					ok: true,
					reply: &AppendEntriesReply{
						Success: false,
						Term:    1,
					},
				},
			},
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.peerCount, c.currentTerm, RoleLeader)
		rpcManager := newFakeRaftRPCManager(c.appendReplies, nil)
		rf.rpcManager = rpcManager

		rf.sendHeartbeat(rf.currentTerm)
		// wait for rpc being called
		time.Sleep(MaxElectionTimeout * time.Millisecond)
		rpcCount := int(atomic.LoadInt32(&(rpcManager.appendEntriesRPCCount)))
		if rpcCount != c.peerCount-1 {
			t.Errorf("case %s expect %d append entries rpc but got %d\n", c.name, c.peerCount-1, rpcCount)
		}

		term, isLeader := rf.GetState()
		if term != c.expectedTerm {
			t.Errorf("case %s expect raft to at term %d but got %d", c.name, c.expectedTerm, term)
		}

		if isLeader != c.isLeader {
			t.Errorf("case %s expect raft to be leader %t, but got %t", c.name, c.isLeader, isLeader)
		}
	}
}
