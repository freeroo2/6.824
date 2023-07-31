package kvraft

import (
	"log"
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
	Key       string
	Value     string
	Typ       OpType
	ClientID  int64
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap              map[string]string
	chanMap            map[int]chan ResultMsg // index(raft) -> chan
	latestRequestIDMap map[int64]int          // clientID -> requestID
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	var cur_index int
	var is_leader bool
	op := Op{Key: args.Key, Typ: GET, ClientID: args.ClientID, RequestID: args.RequestID}
	if cur_index, _, is_leader = kv.rf.Start(op); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	cur_ch, exist := kv.chanMap[cur_index]
	if !exist {
		cur_ch = make(chan ResultMsg, 0)
		kv.chanMap[cur_index] = cur_ch
	}
	kv.mu.Unlock()

	timeout := time.NewTimer(2 * time.Second)
	select {
	case resultMsg := <-cur_ch:
		// do something
		reply.Value = resultMsg.ResultStr
		if reply.Value == "" {
			reply.Err = ErrNoKey
		} else {
			reply.Err = OK
		}

		if !timeout.Stop() {
			<-timeout.C
		}

	case <-timeout.C:
		// handle timeout
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.chanMap, cur_index)
	kv.mu.Unlock()

	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	var cur_index int
	var is_leader bool
	op := Op{Key: args.Key, ClientID: args.ClientID, RequestID: args.RequestID}
	switch args.Op {
	case "Put":
		op.Typ = PUT
	case "Append":
		op.Typ = APPEND
	}
	if cur_index, _, is_leader = kv.rf.Start(op); !is_leader {
		reply.Err = ErrWrongLeader
		return
	}
	cur_ch, exist := kv.chanMap[cur_index]
	if !exist {
		cur_ch = make(chan ResultMsg, 0)
		kv.chanMap[cur_index] = cur_ch
	}
	kv.mu.Unlock()

	timeout := time.NewTimer(2 * time.Second)
	select {
	case <-cur_ch:
		// do something
		reply.Err = OK

		if !timeout.Stop() {
			<-timeout.C
		}

	case <-timeout.C:
		// handle timeout
		reply.Err = ErrWrongLeader
	}

	kv.mu.Lock()
	delete(kv.chanMap, cur_index)
	kv.mu.Unlock()

	return
}

func (kv *KVServer) FetchApplyMsgLoop() {
	for msg := range kv.applyCh {
		if kv.killed() {
			break
		}
		if msg.CommandValid {
			// handle log apply
			kv.HandleCommandFromRaft(msg)
		} else if msg.SnapshotValid {
			// handle snapshot
			kv.HandleSnapshotFromRaft(msg)
		}
	}
}

func (kv *KVServer) HandleCommandFromRaft(msg raft.ApplyMsg) {
	command := msg.Command.(Op)
	resStr := ""
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// a new committed operation that have not seen before, execute it
	if lastSeq, ok := kv.latestRequestIDMap[command.ClientID]; !ok || lastSeq != command.RequestID {
		switch command.Typ {
		case GET:
			resStr = kv.kvMap[command.Key]
		case PUT:
			kv.kvMap[command.Key] = command.Value
		case APPEND:
			kv.kvMap[command.Key] += command.Value
		default:
			// should not happen
		}
		kv.latestRequestIDMap[command.ClientID] = command.RequestID
	} else if lastSeq == command.RequestID && command.Typ == GET {
		resStr = kv.kvMap[command.Key]
	}

	if ch, ok := kv.chanMap[msg.CommandIndex]; ok {
		ch <- ResultMsg{ApplyMsgFromRaft: msg, ResultStr: resStr}
	}
}

func (kv *KVServer) HandleSnapshotFromRaft(msg raft.ApplyMsg) {
	// todo snapshot
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.chanMap = make(map[int]chan ResultMsg)
	kv.kvMap = make(map[string]string)
	kv.latestRequestIDMap = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.FetchApplyMsgLoop()

	return kv
}
