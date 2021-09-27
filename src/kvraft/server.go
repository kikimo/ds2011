package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID ClerkID
	SeqID   SeqID
	OpCode  string
	Key     string
	Value   string // empty if Oprand is Get
	Term    int
}

type ClerkInfo struct {
	ClerkID ClerkID
	SeqID   SeqID
}

type OpAgent struct {
	cmd      Op
	resultCh chan OpResult
}

type OpResult struct {
	Err   Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	clerks      map[ClerkID]*ClerkInfo
	store       map[string]string
	lastApplied int
	resultMap   map[int]*OpAgent
}

func (kv *KVServer) testClerkSeqID(clerkID ClerkID, seqID SeqID) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.clerks[clerkID]; !ok {
		return true
	}

	clerk := kv.clerks[clerkID]
	return clerk.SeqID < seqID
}

// assume kv.mu lock hold
func (kv *KVServer) tryUpdateClerkSeqID(clerkID ClerkID, seqID SeqID) bool {
	if _, ok := kv.clerks[clerkID]; !ok {
		kv.clerks[clerkID] = &ClerkInfo{
			ClerkID: clerkID,
			SeqID:   0,
		}
	}

	clerk := kv.clerks[clerkID]
	if clerk.SeqID == seqID {
		DPrintf("duplicated request of clerk %d with seqID %d found", clerkID, seqID)
		return false
	}

	if clerk.SeqID > seqID {
		DPrintf("stale request of clerk %d with seqID %d found", clerkID, seqID)
		return false
	}

	clerk.SeqID = seqID
	return true
}

// assume kv.mu lock hold
func (kv *KVServer) prepareWaitingForResult(agent *OpAgent, index int) {
	if opAgent, ok := kv.resultMap[index]; ok {
		// DPrintf("clerk %d override stale command of %d at index %d of term %d at term %d", clerkID, opAgent.op.ClerkID, op)
		DPrintf("clerk %d override stale command at index %d at term %d", agent.cmd.ClerkID, index, agent.cmd.Term)
		go func() {
			result := OpResult{
				Err: ErrLeaderChange,
			}
			select {
			case opAgent.resultCh <- result:
			case <-time.After(raft.MaxElectionTimeout * time.Millisecond):
				DPrintf("failed notifying cleirnt of leader change")
			}
		}()
	}

	kv.resultMap[index] = agent
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// reject stale request
	if !kv.testClerkSeqID(args.ClerkID, args.SeqID) {
		reply.Err = ErrStaleRequest
		return
	}
	DPrintf("kv %d handling get command %+v", kv.me, *args)

	cmd := Op{
		ClerkID: args.ClerkID,
		SeqID:   args.SeqID,
		OpCode:  OpGet,
		Key:     args.Key,
		Term:    term,
	}

	// 1. start a raft consencus
	resultCh := make(chan OpResult, 1)
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(cmd) // TODO is this work if Start() return different term
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	// 2. wait for the result on the result channel
	opAgent := OpAgent{
		cmd:      cmd,
		resultCh: resultCh,
	}

	kv.prepareWaitingForResult(&opAgent, index)
	kv.mu.Unlock()
	// for !kv.killed() {
	select {
	case result := <-resultCh:
		reply.Err = result.Err
		reply.Value = result.Value
		DPrintf("kv %d req %+v got reply %+v", kv.me, args, reply)
	case <-time.After(1024 * time.Millisecond):
		reply.Err = ErrResultTimeout
		DPrintf("kv %d timeout waiting Get() at index %d for clerk %d", kv.me, index, args.ClerkID)
	}
	// }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// reject stale request
	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("kv %d handling PutAppend(%+v)", kv.me, *args)
	// DPrintf("kv %d handling put command %+v", kv.me, *args)
	if !kv.testClerkSeqID(args.ClerkID, args.SeqID) {
		reply.Err = ErrStaleRequest
		DPrintf("kv %d stale request %+v", kv.me, *args)
		return
	}

	cmd := Op{
		ClerkID: args.ClerkID,
		SeqID:   args.SeqID,
		OpCode:  args.Op,
		Key:     args.Key,
		Value:   args.Value,
		Term:    term,
	}

	// 1. start a raft consencus
	resultCh := make(chan OpResult, 1)
	kv.mu.Lock()
	index, xterm, ok := kv.rf.Start(cmd)
	DPrintf("kv %d, starting concensus index %d, term %d, isLeader %t, cmd %+v", kv.me, index, xterm, ok, cmd)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("kv %d not leader, return", kv.me)
		kv.mu.Unlock()
		return
	}

	// 2. wait for the result on the result channel
	opAgent := OpAgent{
		cmd:      cmd,
		resultCh: resultCh,
	}

	kv.prepareWaitingForResult(&opAgent, index)
	kv.mu.Unlock()

	// for !kv.killed() {
	select {
	case result := <-resultCh:
		reply.Err = result.Err
		DPrintf("kv %d req %+v sending reply %+v", kv.me, args, reply)
		return
	case <-time.After(1024 * time.Millisecond):
		// ignore
		reply.Err = ErrResultTimeout
		DPrintf("kv %d timeout waiting PutAppend() at index %d for clerk %d", kv.me, index, args.ClerkID)
	}
	// }
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) doExecCmd(msg *raft.ApplyMsg) {
	DPrintf("kv %d handling command %+v", kv.me, *msg)
	cmd, ok := msg.Command.(Op)
	if !ok {
		DPrintf("kv %d illegal msg %+v, cmd: %+v", kv.me, msg, reflect.TypeOf(msg.Command))
		return
	}

	logIndex := msg.CommandIndex
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if msg.CommandIndex <= kv.lastApplied {
		DPrintf("kv %d found duplicated command %+v", kv.me, *msg)
		return
	}
	kv.lastApplied = msg.CommandIndex

	// handle stale or duplicated request
	if !kv.tryUpdateClerkSeqID(cmd.ClerkID, cmd.SeqID) {
		agent, ok := kv.resultMap[logIndex]
		if !ok {
			DPrintf("kv %d, find no agent for msg %+v", kv.me, msg)
			return
		}

		// check term match
		result := OpResult{
			Err: ErrLeaderChange,
		}

		delete(kv.resultMap, logIndex)
		DPrintf("found stale request of clerk %d cmd at index %d term", cmd.ClerkID, logIndex)

		go func() {
			select {
			case agent.resultCh <- result:
			case <-time.After(1 * time.Second): // FIXME magic number
			}
		}()
		// if agent.cmd.Term <= cmd.Term {
		// } else {
		// 	// agent.cmd.Term might be greater of smaller than cmd.Term
		// 	DPrintf("clerk %d cmd at index %d term %d mismatch with agent term %d, it's a stale request", cmd.ClerkID, logIndex, cmd.Term, agent.cmd.Term)
		// }

		return
	}

	result := OpResult{
		Err: OK,
	}
	switch cmd.OpCode {
	case OpGet:
		// we don't care about stale request of Get()
		result.Value, ok = kv.store[cmd.Key]
		DPrintf("kv %d resulting get of key %s is %s, exist %t", kv.me, cmd.Key, result.Value, ok)
		if !ok {
			result.Err = ErrNoKey
		}
	case OpAppend:
		kv.store[cmd.Key] += cmd.Value
		DPrintf("kv %d resulting append of key %s and value %s is %s", kv.me, cmd.Key, cmd.Value, kv.store[cmd.Key])
	case OpPut:
		kv.store[cmd.Key] = cmd.Value
		DPrintf("kv %d resulting put of key %s is %s", kv.me, cmd.Key, cmd.Value)
	default:
		result.Err = ErrUnknownOp
	}

	// notify result
	agent, ok := kv.resultMap[logIndex]
	if !ok {
		DPrintf("kv %d found not agent for msg %+v, return now", kv.me, *msg)
		return
	}

	// 1. term match
	// 2. agent.cmd.Term > cmd.Term
	// 3. agent.cmd.Term < cmd.Term
	// both 2, 3 might happend
	if agent.cmd.Term != cmd.Term {
		DPrintf("kv %d found mismatch term at index %d", kv.me, logIndex)
		result.Err = ErrLeaderChange
		result.Value = ""
	}
	delete(kv.resultMap, logIndex)

	// send back result
	go func() {
		select {
		case agent.resultCh <- result:
		case <-time.After(5 * time.Second): // TODO magic number
		}
	}()
}

func (kv *KVServer) handleSnapshot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kv %d handling snapshot: %+v", kv.me, *msg)
	if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		DPrintf("kv %d failed install snapshot", kv.me)
		return
	}

	kv.loadSnapshot(msg.Snapshot)
}

func (kv *KVServer) execCmd() {
	DPrintf("kv %d command executor running...", kv.me)
	for msg := range kv.applyCh {
		if msg.CommandValid {
			kv.doExecCmd(&msg)
		} else if msg.SnapshotValid {
			kv.handleSnapshot(&msg)
		} else {
			panic(fmt.Sprintf("unknonw command type: %+v", msg))
		}
	}
	DPrintf("kv %d command executor stoped...", kv.me)
}

func (kv *KVServer) doTakeSnapshot() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastApplied := kv.lastApplied
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(&kv.clerks); err != nil {
		panic(fmt.Sprintf("failed encoding clerks: %+v", err))
	}

	if err := e.Encode(&kv.store); err != nil {
		panic(fmt.Sprintf("failed encoding store: %+v", err))
	}

	if err := e.Encode(&lastApplied); err != nil {
		panic(fmt.Sprintf("failed encoding lastApplied: %+v", err))
	}
	data := w.Bytes()
	kv.rf.Snapshot(lastApplied, data)
}

func (kv *KVServer) takeSnapshot() {
	if kv.maxraftstate == -1 {
		return
	}

	for !kv.killed() {
		if kv.persister.RaftStateSize() >= kv.maxraftstate-128 {
			kv.doTakeSnapshot()
		}

		time.Sleep(16 * time.Millisecond) // TODO magic number
	}
}

// assume kv.mu lock hold
func (kv *KVServer) loadSnapshot(data []byte) {
	if kv == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&kv.clerks); err != nil {
		panic(fmt.Sprintf("failed decoding kv clerk: %+v", err))
	}

	if err := d.Decode(&kv.store); err != nil {
		panic(fmt.Sprintf("failed decoding kv store: %+v", err))
	}

	lastApplied := 0
	if err := d.Decode(&lastApplied); err != nil {
		panic(fmt.Sprintf("failed decoding kv lastAppliedIndex: %+v", err))
	}
	kv.lastApplied = lastApplied
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	DPrintf("kv %d maxraftstate %d", kv.me, maxraftstate)

	// You may need initialization code here.

	kv.persister = persister
	if persister.SnapshotSize() > 0 {
		kv.loadSnapshot(persister.ReadSnapshot())
	}
	if kv.clerks == nil {
		kv.clerks = make(map[ClerkID]*ClerkInfo)
	}
	if kv.store == nil {
		kv.store = make(map[string]string)
	}

	kv.resultMap = make(map[int]*OpAgent)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	if maxraftstate != -1 {
		go kv.takeSnapshot()
	}
	go kv.execCmd()
	DPrintf("kv %d running", me)

	// You may need initialization code here.

	return kv
}
