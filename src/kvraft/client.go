package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// we assume no concurrent call to Clerk
	// mu      sync.Mutex
	clerkID ClerkID
	seqID   SeqID
	leader  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = ClerkID(nrand())
	ck.seqID = 1

	return ck
}

func (ck *Clerk) updateLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seqID++

	// try forever
	args := &GetArgs{
		Key:     key,
		ClerkID: ck.clerkID,
		SeqID:   ck.seqID,
	}

	for {
		reply := &GetReply{}
		s := ck.servers[ck.leader]
		succ := s.Call("KVServer.Get", args, reply)
		if !succ {
			DPrintf("clerk %d failed calling Get(%s) to server %d", ck.clerkID, key, ck.leader)
			ck.updateLeader()
			time.Sleep(raft.LeaderIdlePeriod)
			continue
		}

		switch reply.Err {
		case ErrNoKey:
			return ""
		case ErrWrongLeader:
			// try next server
			DPrintf("clerk %d detect wrong leader", ck.clerkID)
			ck.updateLeader()
			// time.Sleep(raft.LeaderIdlePeriod)
		case ErrLeaderChange:
			// try next server
			DPrintf("clerk %d detect leader changed", ck.clerkID)
			ck.updateLeader()
		case ErrResultTimeout:
			ck.seqID++
			args.SeqID = ck.seqID
			ck.updateLeader()
		case ErrStaleRequest:
			ck.seqID++
			args.SeqID = ck.seqID
		case OK:
			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqID++
	args := &PutAppendArgs{
		Key:     key,
		Value:   value,
		Op:      op,
		ClerkID: ck.clerkID,
		SeqID:   ck.seqID,
	}

	for {
		reply := &PutAppendReply{}

		// DPrintf("calling leader %d", ck.leader)
		s := ck.servers[ck.leader]
		succ := s.Call("KVServer.PutAppend", args, reply)
		if !succ {
			DPrintf("clerk %d failed calling PutAppend(%s, %s, %s) to server %d", ck.clerkID, key, value, op, ck.leader)
			// TODO unit test me: try another server if rpc failed
			ck.updateLeader()
			continue
		}

		switch reply.Err {
		case ErrWrongLeader:
			// try next server
			DPrintf("wrong leader %d, try again", ck.leader)
			ck.updateLeader()
			// time.Sleep(raft.LeaderIdlePeriod / 4)
		case ErrLeaderChange:
			// try next server
			ck.updateLeader()
		case ErrResultTimeout:
			// TODO we need timeout to resend request: consider a situation
			// that log has been replicated to all server but not commited,
			// then leader changed, the log will not be commited unless a new
			// log comming

			// TODO unit test me
			// don't increase seqID for PutAppend(), that will result into
			// duplicate result
			args.SeqID = ck.seqID
			ck.updateLeader()
		case ErrStaleRequest:
			// this request has been executed
			return
		case OK:
			DPrintf("put success\n")
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
