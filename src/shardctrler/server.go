package shardctrler

import (
	"fmt"
	"log"
	"sync"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	clerks      map[ClerkID]SeqID
	lastApplied int
	configs     []Config // indexed by config num
	opAgents    map[int]OpAgent
}

type Op struct {
	// Your data here.
	OpType     OpType
	JoinArgsP  *JoinArgs
	LeaveArgsP *LeaveArgs
	MoveArgsP  *MoveArgs
	QueryArgsP *QueryArgs

	// using interface type Args will paralysis rcp object serialization mechanism
	// Args   Args
}

type OpAgent struct {
	resultCh chan OpResult
}

type OpResult struct {
	Err   Err
	Reply interface{}
}

// assume sc.mu lock hold
func (sc *ShardCtrler) installOpAgent(index int, agent *OpAgent) {
	if oldAgent, ok := sc.opAgents[index]; ok {
		DPrintf("command %+v overrided by %+v", oldAgent, agent)
		// TODO we assume that this operation never blocks, but take care
		oldAgent.resultCh <- OpResult{Err: ErrLeaderChanged}
	}

	sc.opAgents[index] = *agent
}

// assume sc.mu lock hold
func (sc *ShardCtrler) tryUpdateClerkInfo(clerkInfo ClerkInfo) bool {
	seqID := sc.clerks[clerkInfo.ClerkID]
	if seqID >= clerkInfo.SeqID {
		return false
	}

	sc.clerks[clerkInfo.ClerkID] = clerkInfo.SeqID
	return true
}

type OpResultCallback func(opResult *OpResult)

func (sc *ShardCtrler) callOp(cmd Op, reply *BaseReply, op OpType, cb OpResultCallback) {
	// Your code here.
	_, isLeader := sc.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	index, term, isLeader := sc.rf.Start(cmd)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		sc.mu.Unlock()
		return
	}
	DPrintf("sc running %s: %+v", op, cmd)

	agent := OpAgent{
		resultCh: make(chan OpResult, 1),
	}
	sc.installOpAgent(index, &agent)
	sc.mu.Unlock()

	// wait for result
	select {
	case <-time.After(4 * time.Second):
		DPrintf("sc timeout waiting joing result, cmd %+v, index %d, term %d", cmd, index, term)
		reply.Err = ErrResultTimeout
	case e := <-agent.resultCh:
		cb(&e)
	}

}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	cb := func(opResult *OpResult) {
		DPrintf("sc join result %+v", opResult)
		if opResult.Reply != nil {
			*reply = opResult.Reply.(JoinReply)
		} else if opResult.Err != "" {
			reply.Err = opResult.Err
		}
	}

	cmd := Op{
		OpType:    OpJoin,
		JoinArgsP: args,
	}
	sc.callOp(cmd, &reply.BaseReply, OpJoin, cb)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cb := func(opResult *OpResult) {
		DPrintf("sc leave result %+v", opResult)
		if opResult.Reply != nil {
			*reply = opResult.Reply.(LeaveReply)
		} else if opResult.Err != "" {
			reply.Err = opResult.Err
		}
	}

	cmd := Op{
		OpType:     OpLeave,
		LeaveArgsP: args,
	}
	sc.callOp(cmd, &reply.BaseReply, OpLeave, cb)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cb := func(opResult *OpResult) {
		DPrintf("sc move result %+v", opResult)
		if opResult.Reply != nil {
			*reply = opResult.Reply.(MoveReply)
		} else if opResult.Err != "" {
			reply.Err = opResult.Err
		}
	}

	cmd := Op{
		OpType:    OpMove,
		MoveArgsP: args,
	}
	sc.callOp(cmd, &reply.BaseReply, OpMove, cb)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cb := func(opResult *OpResult) {
		DPrintf("sc query result %+v", opResult)
		if opResult.Reply != nil {
			*reply = opResult.Reply.(QueryReply)
		} else if opResult.Err != "" {
			reply.Err = opResult.Err
		}
	}

	cmd := Op{
		OpType:     OpQuery,
		QueryArgsP: args,
	}
	sc.callOp(cmd, &reply.BaseReply, OpQuery, cb)
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// assume sc.mu lock hold
func (sc *ShardCtrler) handleJoin(op Op, commandIndex int) {
	args := op.JoinArgsP
	latestConfig := sc.latestConfig()
	newConfig := latestConfig.Clone()
	newConfig.Num = latestConfig.Num + 1
	newConfig.Join(args.Servers)
	newConfig.BalanceShard()
	sc.configs = append(sc.configs, newConfig)

	if agent, ok := sc.opAgents[commandIndex]; ok {
		agent.resultCh <- OpResult{
			Reply: JoinReply{},
		}

		delete(sc.opAgents, commandIndex)
	} else {
		DPrintf("sc found no agent fot op: %+v at %d", op, commandIndex)
	}
}

// assume sc.mu lock hold
func (sc *ShardCtrler) handleLeave(op Op, commandIndex int) {
	args := op.LeaveArgsP
	latestConfig := sc.latestConfig()
	newConfig := latestConfig.Clone()
	newConfig.Num = latestConfig.Num + 1
	newConfig.Leave(args.GIDs)
	newConfig.BalanceShard()
	sc.configs = append(sc.configs, newConfig)

	if agent, ok := sc.opAgents[commandIndex]; ok {
		agent.resultCh <- OpResult{
			Reply: LeaveReply{},
		}

		delete(sc.opAgents, commandIndex)
	} else {
		DPrintf("sc found no agent fot op: %+v at %d", op, commandIndex)
	}
}

func (sc *ShardCtrler) handleQuery(op Op, commandIndex int) {
	args := op.QueryArgsP
	sz := len(sc.configs)
	num := args.Num
	if num == -1 {
		num = sz - 1
	}

	reply := QueryReply{}
	if agent, ok := sc.opAgents[commandIndex]; ok {
		if num < 0 || num >= sz {
			reply.Err = ErrBadRequest
		} else {
			reply.Config = sc.configs[num]
			DPrintf("sc serving config for %d: %+v, all configs: %+v", num, reply.Config, sc.configs)
		}

		delete(sc.opAgents, commandIndex)
		agent.resultCh <- OpResult{Reply: reply}
	} else {
		DPrintf("sc found no agent fot op: %+v at %d", op, commandIndex)
	}
}

func (sc *ShardCtrler) latestConfig() *Config {
	sz := len(sc.configs)
	return &sc.configs[sz-1]
}

func (sc *ShardCtrler) handleMove(op Op, commandIndex int) {
	// TODO check result
	args := op.MoveArgsP
	latestConfig := sc.latestConfig()
	newConfig := latestConfig.Clone()
	newConfig.Num = latestConfig.Num + 1
	reply := MoveReply{}
	if !newConfig.Move(args.GID, args.Shard) {
		DPrintf("sc error move shard %d to group %d", args.Shard, args.Shard)
		reply.Err = ErrBadRequest
	} else {
		sc.configs = append(sc.configs, newConfig)
	}

	if agent, ok := sc.opAgents[commandIndex]; ok {
		agent.resultCh <- OpResult{Reply: reply}
		delete(sc.opAgents, commandIndex)
	} else {
		DPrintf("sc found no agent fot op: %+v at %d", op, commandIndex)
	}
}

type CMDHandler func(Op, int)

func (sc *ShardCtrler) handleCmd(msg *raft.ApplyMsg) {
	op := msg.Command.(Op)
	// args := op.Args.(JoinArgs)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if msg.CommandIndex <= sc.lastApplied {
		DPrintf("sc stale command %+v, lastApplied %d\n", msg, sc.lastApplied)
		return
	}

	sc.lastApplied = msg.CommandIndex
	var cmdHandler CMDHandler
	var clerkInfo ClerkInfo
	switch op.OpType {
	case OpJoin:
		cmdHandler = sc.handleJoin
		clerkInfo = op.JoinArgsP.GetClerk()
	case OpLeave:
		cmdHandler = sc.handleLeave
		clerkInfo = op.LeaveArgsP.GetClerk()
	case OpQuery:
		cmdHandler = sc.handleQuery
		clerkInfo = op.QueryArgsP.GetClerk()
	case OpMove:
		cmdHandler = sc.handleMove
		clerkInfo = op.MoveArgsP.GetClerk()
	default:
		panic(fmt.Sprintf("unknonw op type: %s", op.OpType))
	}

	if !sc.tryUpdateClerkInfo(clerkInfo) {
		// stale req
		if agent, ok := sc.opAgents[msg.CommandIndex]; ok {
			// TODO check term?
			agent.resultCh <- OpResult{Err: ErrStaleRequest}
		} else {
			DPrintf("sc found no agent for msg: %+v", msg)
		}

		return
	}
	cmdHandler(op, msg.CommandIndex)
}

func (sc *ShardCtrler) execCmd() {
	for msg := range sc.applyCh {
		if msg.CommandIndex <= sc.lastApplied {
			continue
		}

		if !msg.CommandValid {
			DPrintf("sc ignore invalid command: %+v", msg)
			continue
		}

		sc.handleCmd(&msg)
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clerks = map[ClerkID]SeqID{}
	sc.opAgents = map[int]OpAgent{}
	go sc.execCmd()

	return sc
}
