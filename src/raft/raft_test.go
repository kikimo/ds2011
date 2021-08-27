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
	log      []LogEntry
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

func newTestRaft(status *raftStatus) *Raft {
	rf := &Raft{}
	rf.log = status.log
	if len(rf.log) == 0 {
		nullEnt := LogEntry{
			Index: 0,
			Term:  0,
		}
		rf.log = append(rf.log, nullEnt)
	}

	rf.peers = make([]*labrpc.ClientEnd, status.numPeers)
	rf.me = 0
	rf.role = status.role
	rf.currentTerm = status.term
	rf.votedFor = status.votedFor
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// start ticker goroutine to start elections
	rf.hbChan = make(chan hbParams)
	rf.rvChan = make(chan rvParams)
	// rf.applyCh = nil

	return rf

}

func TestRunFollowerUT(t *testing.T) {
	// cases:
	// 1. eto time out, become candidate
	// 2. receive hb msg from leader
	// 3. receive stale hb msg
	// 4. receive rv msg and grant vote
	// 5. receive rv msg but not vote granted
	// 6. receive stale rv msg
	cases := []struct {
		name           string
		eto            time.Duration
		testRaftStatus *raftStatus
		testRVParams   *rvParams
		testHBParams   *hbParams
		expectedRole   RaftRole
		expectedTerm   int
		timeout        bool
	}{
		{
			name: "follower become candidate",
			eto:  MinElectionTimeout * time.Millisecond,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     0,
				votedFor: 0,
				role:     RoleFollower,
			},
			expectedRole: RoleCandidate,
			expectedTerm: 0,
			timeout:      true,
		},
		{
			name: "follower recieve legal heartbeat msg",
			eto:  MinElectionTimeout * time.Millisecond * 2,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     1,
				votedFor: 1,
				role:     RoleFollower,
			},
			expectedRole: RoleFollower,
			expectedTerm: 1,
			testHBParams: &hbParams{
				appendEntriesArgs: AppendEntriesArgs{
					Term:     2,
					LeaderID: 1,
				},
			},
			timeout: false,
		},
		{
			name: "follower recieve stale heartbeat msg",
			eto:  MinElectionTimeout * time.Millisecond,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     3,
				votedFor: 1,
				role:     RoleFollower,
			},
			expectedRole: RoleCandidate,
			expectedTerm: 3,
			testHBParams: &hbParams{
				appendEntriesArgs: AppendEntriesArgs{
					Term:     2,
					LeaderID: 1,
				},
			},
			timeout: true,
		},
		{
			name: "follower grant vote",
			eto:  MinElectionTimeout * time.Millisecond,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     3,
				votedFor: 1,
				role:     RoleFollower,
			},
			expectedRole: RoleFollower,
			expectedTerm: 3,
			testRVParams: &rvParams{
				requestVoteArgs: RequestVoteArgs{
					CandiateID: 1,
					Term:       4,
				},
				requestVoteReply: RequestVoteReply{
					VoteGranted: true,
					Term:        3,
				},
			},
			timeout: false,
		},
		{
			name: "follower not grant vote",
			eto:  MinElectionTimeout * time.Millisecond,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     3,
				votedFor: 1,
				role:     RoleFollower,
			},
			expectedRole: RoleCandidate,
			expectedTerm: 3,
			testRVParams: &rvParams{
				requestVoteArgs: RequestVoteArgs{
					CandiateID: 1,
					Term:       3,
				},
				requestVoteReply: RequestVoteReply{
					VoteGranted: false,
					Term:        3,
				},
			},
			timeout: true,
		},
		{
			name: "follower recieve stale vote request",
			eto:  MinElectionTimeout * time.Millisecond,
			testRaftStatus: &raftStatus{
				numPeers: 3,
				term:     3,
				votedFor: 1,
				role:     RoleFollower,
			},
			expectedRole: RoleCandidate,
			expectedTerm: 3,
			testRVParams: &rvParams{
				requestVoteArgs: RequestVoteArgs{
					CandiateID: 1,
					Term:       2,
				},
				requestVoteReply: RequestVoteReply{
					VoteGranted: false,
					Term:        3,
				},
			},
			timeout: true,
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.testRaftStatus)
		if c.testHBParams != nil {
			go func() {
				select {
				case rf.hbChan <- *c.testHBParams:
				case <-time.After(MaxElectionTimeout * time.Millisecond):
				}
			}()
		}

		if c.testRVParams != nil {
			go func() {
				select {
				case rf.rvChan <- *c.testRVParams:
				case <-time.After(MaxElectionTimeout * time.Millisecond):
				}
			}()
		}

		now := time.Now()
		rf.runFollower(rf.currentTerm, c.eto)
		delta := time.Since(now)
		if c.timeout {
			if delta < c.eto {
				t.Errorf("Error running %s, expect follower eto but not", c.name)
			}
		} else {
			if delta > c.eto {
				t.Errorf("Error running %s, expect not follower eto but not", c.name)
			}
		}

		if rf.role != c.expectedRole {
			t.Errorf("Error running %s, expect raft role to be %s but got %s", c.name, c.expectedRole, rf.role)
		}

		if rf.currentTerm != c.expectedTerm {
			t.Errorf("Error running %s, expect raft term to be %d but got %d", c.name, c.expectedTerm, rf.currentTerm)
		}

	}
}

func TestRunCandidateUT(t *testing.T) {
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

func TestRunLeaderUT(t *testing.T) {
	// TODO
	// cases:
	// 1. hbto time out, send hb again
	// 2. recieve stale hb msg
	// 3. recieve hb msg from leader
	// 4. recieve stale rv msg
	// 5. recieve rv msg but not vote granted
	// 6. recieve rv msg and grant vote
}

func TestAppendEntriesUT(t *testing.T) {
	// cases:
	//  1. ~~ignore stale request~~
	//  2. ~~follower handle normal request(should resset election timer)~~
	//  3. ~~candidate handle normal request, update term and convert to follower~~
	cases := []struct {
		name              string
		expectedRaftTerm  int
		expectedReplyTerm int
		expectedSuccess   bool
		expectedRaftRole  RaftRole
		rfStatus          *raftStatus
		args              *AppendEntriesArgs
		reply             *AppendEntriesReply
		bolck             bool
	}{
		{
			name:              "Ignore stale append entries",
			expectedRaftTerm:  1,
			expectedReplyTerm: 1,
			expectedRaftRole:  RoleFollower,
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     1,
				role:     RoleFollower,
			},
			expectedSuccess: false,
			args: &AppendEntriesArgs{
				Term:     0,
				LeaderID: 1,
			},
			reply: &AppendEntriesReply{},
			bolck: true,
		},
		{
			name: "Normal hb request",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     1,
				role:     RoleFollower,
			},
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
			name: "Candidate handle normal request, update term and convert to follower",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     0,
				role:     RoleCandidate,
			},
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
		rf := newTestRaft(c.rfStatus)
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

func TestRequestVoteUT(t *testing.T) {
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
		rf := newTestRaft(c.rfStatus)
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

func TestStartElectionUT(t *testing.T) {
	// cases:
	//  1. win an election
	//  2. election tie
	//  3. lost an election(recieve higher term and convert to follower)
	cases := []struct {
		name        string
		rfStatus    *raftStatus
		voteReplies []*testRequestVoteReply
		// expectedRole RaftRole
		// expectedTerm int
		win bool
	}{
		{
			name: "win election",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     0,
			},
			voteReplies: []*testRequestVoteReply{
				{
					ok: true,
					reply: &RequestVoteReply{
						Term:        0,
						VoteGranted: true,
					},
				},
				{
					ok: true,
					reply: &RequestVoteReply{
						Term:        0,
						VoteGranted: false,
					},
				},
			},
			// expectedRole: RoleLeader,
			// expectedTerm: 0,
			win: true,
		},
		{
			name: "election tie",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     0,
			},
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
			name: "lose an election",
			rfStatus: &raftStatus{
				numPeers: 3,
				term:     0,
			},
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
		rf := newTestRaft(c.rfStatus)
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

func TestSendHeartbeatUT(t *testing.T) {
	// cases:
	//  1. recieve same term and remain leader
	//  2. recieve higher term and convert to follower
	cases := []struct {
		name          string
		rfStatus      *raftStatus
		isLeader      bool
		expectedTerm  int
		appendReplies []*testAppendEntriesReply
	}{
		{
			name: "'remain leader'",
			rfStatus: &raftStatus{
				role:     RoleLeader,
				numPeers: 3,
				term:     0,
			},
			isLeader:     true,
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
			name: "'converted to follower'",
			rfStatus: &raftStatus{
				role:     RoleLeader,
				numPeers: 3,
				term:     0,
			},
			isLeader:     false,
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
		rf := newTestRaft(c.rfStatus)
		rpcManager := newFakeRaftRPCManager(c.appendReplies, nil)
		rf.rpcManager = rpcManager

		sendHBChan := make(chan hbParams)
		// TODO test append entries
		rf.sendHeartbeat(rf.currentTerm, sendHBChan, nil, true)
		// wait for rpc being called
		time.Sleep(MaxElectionTimeout * time.Millisecond)
		rpcCount := int(atomic.LoadInt32(&(rpcManager.appendEntriesRPCCount)))
		if rpcCount != c.rfStatus.numPeers-1 {
			t.Errorf("case %s expect %d append entries rpc but got %d\n", c.name, c.rfStatus.numPeers-1, rpcCount)
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

func TestCompareLogUT(t *testing.T) {
	// cases:
	// 	1. same term, same index
	//  2. higher term, lower index
	//  3. lower term, higher index
	cases := []struct {
		name         string
		lastLogIndex int
		lastLogTerm  int
		rfStatus     *raftStatus
		success      bool
	}{
		{
			name:         "same term same index",
			lastLogTerm:  3,
			lastLogIndex: 4,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Term:  3,
						Index: 4,
					},
				},
			},
			success: true,
		},
		{
			name:         "higher term lower index",
			lastLogTerm:  4,
			lastLogIndex: 3,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Term:  3,
						Index: 4,
					},
				},
			},
			success: true,
		},
		{
			name:         "lower term higher index",
			lastLogTerm:  2,
			lastLogIndex: 5,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Term:  3,
						Index: 4,
					},
				},
			},
			success: false,
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.rfStatus)
		success := rf.compareLog(c.lastLogTerm, c.lastLogIndex)
		if success != c.success {
			t.Errorf("Error testing %s, expect result %t but got %t", c.name, c.success, success)
		}
	}
}

func TestAppendLogUT(t *testing.T) {
	// cases:
	// 	1. all match and longer
	// 	2. all match and shorter
	//  3. mismatch and longer
	//  4. mismatch and shorter
	cases := []struct {
		name         string
		prevLogIndex int
		entries      []LogEntry
		match        bool
		size         int
		rfStatus     *raftStatus
	}{
		{
			name:         "all match and longer",
			prevLogIndex: 2,
			entries: []LogEntry{
				{
					Index: 3,
					Term:  4,
				},
				{
					Index: 4,
					Term:  4,
				},
			},
			match: true,
			size:  4,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Index: 1,
						Term:  2,
					},
					{
						Index: 2,
						Term:  2,
					},
					{
						Index: 3,
						Term:  4,
					},
				},
			},
		},
		{
			name:         "all match and shorter",
			prevLogIndex: 2,
			entries: []LogEntry{
				{
					Index: 3,
					Term:  4,
				},
			},
			match: true,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Index: 1,
						Term:  2,
					},
					{
						Index: 2,
						Term:  2,
					},
					{
						Index: 3,
						Term:  4,
					},
					{
						Index: 4,
						Term:  5,
					},
				},
			},
			size: 4,
		},
		{
			name:         "mismatch and longer",
			size:         4,
			prevLogIndex: 1,
			entries: []LogEntry{
				{
					Index: 2,
					Term:  3,
				},
				{
					Index: 3,
					Term:  4,
				},
				{
					Index: 4,
					Term:  4,
				},
			},
			match: false,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Index: 1,
						Term:  2,
					},
					{
						Index: 2,
						Term:  2,
					},
					{
						Index: 3,
						Term:  4,
					},
				},
			},
		},
		{
			name:         "mismatch and shorter",
			size:         3,
			prevLogIndex: 1,
			entries: []LogEntry{
				{
					Index: 2,
					Term:  3,
				},
				{
					Index: 3,
					Term:  4,
				},
			},
			match: false,
			rfStatus: &raftStatus{
				log: []LogEntry{
					{
						Index: 1,
						Term:  2,
					},
					{
						Index: 2,
						Term:  2,
					},
					{
						Index: 3,
						Term:  4,
					},
					{
						Index: 4,
						Term:  5,
					},
				},
			},
		},
	}

	for _, c := range cases {
		rf := newTestRaft(c.rfStatus)
		match := rf.appendLog(c.prevLogIndex, c.entries)
		if match != c.match {
			t.Errorf("Error testing %s, expect log match to be %t but got %t", c.name, c.match, match)
		}

		sz := len(rf.log)
		if sz != c.size {
			t.Errorf("Error testing %s, expect log growth to be of size %d but got %d", c.name, c.size, sz)
		}
	}
}
