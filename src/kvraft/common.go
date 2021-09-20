package kvraft

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrLeaderChange  = "ErrLeaderChange"
	ErrStaleRequest  = "ErrStaleRequest" // a request that has been executed, but somehow the leader failed to send back the request
	ErrUnknownOp     = "ErrUnknownOp"
	ErrResultTimeout = "ErrResultTimeout"
)

type ClerkID int64
type SeqID int64

const (
	OpPut    = "Put"
	OpAppend = "Append"
	OpGet    = "Get"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID ClerkID
	SeqID   SeqID
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID ClerkID
	SeqID   SeqID
}

type GetReply struct {
	Err   Err
	Value string
}
