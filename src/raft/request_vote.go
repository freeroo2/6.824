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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// logger.PrettyDebug(logger.DVote, "S%d: receive a RequestVote from %v, args term %v, rf.currentTerm %v", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	// rules for servers
	// all servers 2
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	// request vote rpc receiver 1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// request vote rpc receiver 2
	myLastLog := rf.log.lastLog()
	upToDate := args.LastLogTerm > myLastLog.Term || (args.LastLogTerm == myLastLog.Term && args.LastLogIndex >= myLastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.ResetElectionTimeOut()
		logger.PrettyDebug(logger.DVote, "S%v: term %v vote %v", rf.me, rf.currentTerm, rf.votedFor)
	} else {
		reply.VoteGranted = false
	}
	// after update
	reply.Term = rf.currentTerm
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
	logger.PrettyDebug(logger.DVote, "S%d: term %v send vote request to %d", rf.me, args.Term, serverID)
	var reply RequestVoteReply
	ok := rf.sendRequestVote(serverID, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		logger.PrettyDebug(logger.DVote, "S%d: %d 在新的term，更新term，结束", rf.me, serverID)
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term < args.Term {
		logger.PrettyDebug(logger.DVote, "S%d: %d 的term %d 已经失效，结束\n", rf.me, serverID, reply.Term)
		return
	}
	if !reply.VoteGranted {
		logger.PrettyDebug(logger.DVote, "S%d: %d 没有投给me，结束\n", rf.me, serverID)
		return
	}

	logger.PrettyDebug(logger.DVote, "S%d: reply from %d, term一致，且投给%d", rf.me, serverID, rf.me)
	atomic.AddInt64(votesCount, 1)
	n := len(rf.peers)
	if *votesCount > int64(n/2) && args.Term == rf.currentTerm && rf.status == CANDIDATE {
		becomesLeader.Do(func() {

			rf.status = LEADER
			logger.PrettyDebug(logger.DLeader, "S%d: come to power, start to send heartbeat", rf.me)
			for i := 0; i < n; i++ {
				rf.nextIndex[i] = rf.log.lastLog().Index + 1
				rf.matchIndex[i] = 0
			}
			// logger.PrettyDebug(logger.DLeader, "S%d: leader - nextIndex %#v", rf.me, rf.nextIndex)
			rf.leaderAppendEntries(true)
		})
	}
}
