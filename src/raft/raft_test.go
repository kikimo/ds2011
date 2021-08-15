package raft

import "testing"

func TestRunFollower(t *testing.T) {
	// TODO
	// cases:
	// 1. eto time out, become candidate
	// 2. receive hb msg
	// 3. receive stale hb msg
	// 4. receive rv msg and grant vote
	// 5. receive rv msg but not vote granted
	// 6. receive stale rv msg
}

func TestRunCandidate(t *testing.T) {
	// TODO
	// cases:
	// 1.
}

func TestRunLeader(t *testing.T) {
	// TODO
	// cases:
	// 1.
}
