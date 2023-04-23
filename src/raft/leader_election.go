package raft

import (
	"sync"
	"sync/atomic"

	"6.824/logger"
)

func (rf *Raft) setNewTerm(newTerm int) {
	if newTerm > rf.currentTerm || rf.currentTerm == 0 {
		rf.currentTerm = newTerm
		rf.status = FOLLOWER
		rf.votedFor = -1
		logger.PrettyDebug(logger.DTerm, "S%d, set new term %d", rf.me, newTerm)
	}
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

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	var votesCount int64
	votesCount = 1
	//lastlog := rf.log.lastLog()
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//LastLogIndex: lastlog.Index,
		//LastLogTerm:  lastlog.Term,
	}
	var becomesLeader sync.Once
	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.candidateRequestVote(serverID, &args, &votesCount, &becomesLeader)
		}
	}

}
