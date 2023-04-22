package raft

import "6.824/logger"

func (rf *Raft) setNewTerm(newTerm int) {
	if newTerm > rf.currentTerm || rf.currentTerm == 0 {
		rf.currentTerm = newTerm
		rf.status = FOLLOWER
		rf.votedFor = -1
		logger.PrettyDebug(logger.DTerm, "S%d, set new term %d", rf.me, newTerm)
	}
}

func (rf *Raft) candidateRequestVote(serverID int, args *RequestVoteArgs, votesCount *int) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(serverID, args, &reply)
	if !ok {
		return
	}
	if reply.Term > args.Term {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.status = FOLLOWER
	}
}

func (rf *Raft) leaderElection() {
	rf.currentTerm++
	rf.status = CANDIDATE
	rf.votedFor = rf.me
	votesCount := 1
	lastlog := rf.log.lastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastlog.Index,
		LastLogTerm:  lastlog.Term,
	}

	for serverID, _ := range rf.peers {
		if serverID != rf.me {
			go rf.candidateRequestVote(serverID, &args, &votesCount)
		}
	}

}
