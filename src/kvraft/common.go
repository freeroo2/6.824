package kvraft

import "6.824/raft"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type OpType int32

const (
	PUT    OpType = 0
	GET    OpType = 1
	APPEND OpType = 2
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
	Typ       OpType
	ClientID  int64
	RequestID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Typ       OpType
	ClientID  int64
	RequestID int
}

type GetReply struct {
	Err   Err
	Value string
}

type ResultMsg struct {
	ApplyMsgFromRaft raft.ApplyMsg
	ResultStr        string
}
