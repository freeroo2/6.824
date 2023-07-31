package kvraft

import (
	"crypto/rand"
	"math/big"
	math_rand "math/rand"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastestLeaderID int
	clientID        int64
	requestID       int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func getRandomServer(length int) int {
	math_rand.Seed(time.Now().UnixNano())
	return math_rand.Intn(length)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.requestID++
	args := GetArgs{Key: key, Typ: GET, ClientID: ck.clientID, RequestID: ck.requestID}
	server := ck.lastestLeaderID
	for {
		reply := GetReply{}
		// You will have to modify this function.
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = getRandomServer(len(ck.servers))
			continue
		}
		if reply.Err == ErrNoKey {
			return ""
		}
		if reply.Err == OK {
			ck.lastestLeaderID = server
			return reply.Value
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestID++
	server := ck.lastestLeaderID
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, RequestID: ck.requestID}
	for {
		reply := PutAppendReply{}
		// You will have to modify this function.
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader {
			server = getRandomServer(len(ck.servers))
			continue
		}
		if reply.Err == OK {
			ck.lastestLeaderID = server
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
