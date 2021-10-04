package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	leader int
	ClerkInfo
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

	// Your code here.
	ck.ClerkID = ClerkID(nrand())
	ck.SeqID = 0
	ck.leader = 0

	return ck
}

func (ck *Clerk) updateLeader() {
	ck.leader = (ck.leader + 1) % len(ck.servers)
}

func (ck *Clerk) currLeader() *labrpc.ClientEnd {
	return ck.servers[ck.leader]
}

func (ck *Clerk) updateAndGetSeqID() SeqID {
	seqID := atomic.AddInt64((*int64)(&ck.SeqID), 1)
	return SeqID(seqID)
}

type ReplyFactory func() Reply

func (ck *Clerk) callQuery(method string, args Args, replyFact ReplyFactory) Reply {
	for {
		// try each known server.
		reply := replyFact()
		ok := ck.currLeader().Call(method, args, reply)
		if ok {
			switch reply.GetErr() {
			case ErrNone:
				return reply
			case ErrStaleRequest:
				// try again with new seqID
				seqID := ck.updateAndGetSeqID()
				args.SetSeqID(seqID)
			case ErrWrongLeader:
				// try another server
				ck.updateLeader()
			case ErrLeaderChanged:
				// try again
			case ErrResultTimeout:
				// try again
			case ErrBadRequest:
				panic(fmt.Sprintf("bad request: %+v", args))
			default:
				panic(fmt.Sprintf("unknown reply code: %+v", reply))
			}
		} else {
			ck.updateLeader()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) callExec(method string, args Args, replyFact ReplyFactory) Reply {
	for {
		// try each known server.
		reply := replyFact()
		ok := ck.currLeader().Call(method, args, reply)
		if ok {
			switch reply.GetErr() {
			case ErrNone:
				return reply
			case ErrStaleRequest:
				// try again with new seqID
				return reply
			case ErrWrongLeader:
				// try another server
				ck.updateLeader()
			case ErrLeaderChanged:
				// try again
			case ErrResultTimeout:
				// try again
			case ErrBadRequest:
				panic(fmt.Sprintf("bad request: %+v", args))
			default:
				panic(fmt.Sprintf("unknown reply code: %+v", reply))
			}
		} else {
			ck.updateLeader()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func QueryReplyFactory() Reply {
	return &QueryReply{}
}

func JoinReplyFactory() Reply {
	return &JoinReply{}
}

func LeaveReplyFactory() Reply {
	return &LeaveReply{}
}

func MoveReplyFactory() Reply {
	return &MoveReply{}
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.SeqID = ck.updateAndGetSeqID()
	args.ClerkID = ck.ClerkID

	reply := ck.callQuery("ShardCtrler.Query", args, QueryReplyFactory)
	return reply.(*QueryReply).Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.SeqID = ck.updateAndGetSeqID()
	args.ClerkID = ck.ClerkID
	ck.callExec("ShardCtrler.Join", args, JoinReplyFactory)
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClerkID = ck.ClerkID
	args.SeqID = ck.updateAndGetSeqID()
	ck.callExec("ShardCtrler.Leave", args, LeaveReplyFactory)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClerkID = ck.ClerkID
	args.SeqID = ck.updateAndGetSeqID()
	ck.callExec("ShardCtrler.Move", args, MoveReplyFactory)
}
