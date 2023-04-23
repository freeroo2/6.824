package raft

import (
	"sync"
	"sync/atomic"

	"6.824/logger"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) candidateRequestVote(serverID int, args *RequestVoteArgs, votesCount *int64, becomesLeader *sync.Once) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(serverID, args, &reply)
	if !ok {
		return
	}
	if reply.Term > args.Term {
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		return
	}
	atomic.AddInt64(votesCount, 1)
	n := len(rf.peers)
	if *votesCount > int64(n/2) {
		becomesLeader.Do(func() {
			rf.status = LEADER
			logger.PrettyDebug(logger.DLeader, "S%d, come to power, start to send heartbeat", rf.me)
			//rf.ResetElectionTimeOut()
			for i := 0; i < n; i++ {
				rf.nextIndex[i] = rf.log.lastLog().Index + 1
				rf.matchIndex[i] = 0
			}
			rf.leaderAppendEntries(true)
		})
	}
}
