package raft

import "6.824/logger"

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	logger.PrettyDebug(logger.DInfo, "S%d: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
	reply.Success = false
	// rules for servers
	// all servers 2
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
		return
	}
	// append entries rpc 1
	if args.Term < rf.currentTerm {
		return
	}
	rf.ResetElectionTimeOut()
	//todo
	prevLog := rf.log.at(args.PrevLogIndex)
	if prevLog == nil || prevLog.Term != args.Term {
		return
	}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntries(isHeartbeat bool) {
	lastLog := rf.log.lastLog()
	for peerID, _ := range rf.peers {
		if peerID == rf.me {
			rf.ResetElectionTimeOut()
			continue
		}

		if lastLog.Index >= rf.nextIndex[peerID] || isHeartbeat {
			nextIndex := rf.nextIndex[peerID]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				LeaderCommit: rf.commitIndex,
				Entries:      make([]*Entry, lastLog.Index-nextIndex+1),
			}
			copy(args.Entries, rf.log.slice(nextIndex))
			go rf.leaderSendEntries(peerID, &args)
		}
	}
}
func (rf *Raft) leaderSendEntries(serverID int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverID, args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term == rf.currentTerm {
		if reply.Success {
			//todo
		}
	}

}
