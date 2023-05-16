package raft

import (
	"sync"

	"6.824/logger"
)

func (rf *Raft) setNewTerm(newTerm int) {
	if newTerm > rf.currentTerm || rf.currentTerm == 0 {
		rf.currentTerm = newTerm
		rf.status = FOLLOWER
		rf.votedFor = -1
		logger.PrettyDebug(logger.DTerm, "S%d: set new term %d", rf.me, newTerm)
	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	rf.ResetElectionTimeOut()
	var votesCount int64
	votesCount = 1
	lastlog := rf.log.lastLog()
	logger.PrettyDebug(logger.DInfo, "S%v: start leader election, term %d", rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastlog.Index,
		LastLogTerm:  lastlog.Term,
	}
	var becomesLeader sync.Once
	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.candidateRequestVote(serverID, &args, &votesCount, &becomesLeader)
		}
	}

}
