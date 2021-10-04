package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK               = "OK"
	ErrNone          = ""
	ErrWrongLeader   = "ErrWrongLeader"
	ErrLeaderChanged = "ErrLeaderChanged"
	ErrResultTimeout = "ErrResultTimeout"
	ErrStaleRequest  = "ErrStaleReques"
	ErrBadRequest    = "ErrBadRequest"
)

type Err string
type ClerkID int64
type SeqID int64
type OpType string

const (
	OpJoin  = "join"
	OpLeave = "leave"
	OpQuery = "query"
	OpMove  = "move"
)

type ClerkInfo struct {
	ClerkID ClerkID
	SeqID   SeqID
}

type Reply interface {
	IsWrongLeader() bool
	GetErr() Err
}

type Args interface {
	// GetSeqID() SeqID
	SetSeqID(seqID SeqID)
	GetClerk() ClerkInfo
}

type BaseReply struct {
	WrongLeader bool
	Err         Err
}

type BaseArgs struct {
	ClerkInfo
}

func (a *BaseArgs) GetClerk() ClerkInfo {
	return a.ClerkInfo
}

func (a *BaseArgs) SetSeqID(seqID SeqID) {
	a.SeqID = seqID
}

func (r *BaseReply) GetErr() Err {
	return r.Err
}

func (r *BaseReply) IsWrongLeader() bool {
	return r.WrongLeader
}

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	BaseArgs
}

type JoinReply struct {
	BaseReply
}

type LeaveArgs struct {
	GIDs []int
	BaseArgs
}

type LeaveReply struct {
	BaseReply
}

type MoveArgs struct {
	Shard int
	GID   int
	BaseArgs
}

type MoveReply struct {
	BaseReply
}

type QueryArgs struct {
	Num int // desired config number
	BaseArgs
}

type QueryReply struct {
	Config Config
	BaseReply
}
