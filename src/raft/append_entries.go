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
	Term     int
	Success  bool
	Conflict bool
	Xindex   int
	Xterm    int
	Xlen     int
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
	// Candidates 3
	if rf.status == CANDIDATE {
		rf.status = FOLLOWER

	}
	// appendentries rpc 2
	prevLog := rf.log.at(args.PrevLogIndex)
	if rf.log.lastLog().Index < args.PrevLogIndex {
		reply.Conflict = true
		reply.Xindex = -1
		reply.Xterm = -1
		reply.Xlen = len(rf.log.Entries)
		logger.PrettyDebug(logger.DConflict, "S%v: Conflict Xterm %v, Xindex %v, Xlen %v", rf.me, reply.Xterm, reply.Xindex, reply.Xlen)
		return
	}
	if prevLog.Term != args.PrevLogTerm {
		reply.Conflict = true
		xterm := prevLog.Term
		// " If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs. "
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term,
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		// here, conflictTerm = reply.Xterm
		for xindex := args.PrevLogIndex; xindex > 0; xindex-- {
			if rf.log.at(xindex-1).Term != xterm {
				reply.Xindex = xindex
				break
			}
		}
		reply.Xterm = xterm
		reply.Xlen = len(rf.log.Entries)
		logger.PrettyDebug(logger.DConflict, "S%v: Conflict Xterm %v, Xindex %v, Xlen %v", rf.me, reply.Xterm, reply.Xindex, reply.Xlen)
		return
	}
	for index, entry := range args.Entries {
		// append entries rpc 3
		//logger.PrettyDebug(logger.DInfo, "S%d: index=%d, rf.log.lastLog().Index=%d, entry.Term=%d, rf.log.at(entry.Index).Term=%d", rf.me, index, rf.log.lastLog().Index, entry.Term, rf.log.at(entry.Index).Term)
		if entry.Index <= rf.log.lastLog().Index && entry.Term != rf.log.at(entry.Index).Term {
			rf.log.truncate(entry.Index)
			logger.PrettyDebug(logger.DInfo, "S%d: follower remove slice from Index %v", rf.me, index)
		}
		// append entries rpc 4
		if entry.Index > rf.log.lastLog().Index {
			rf.log.append(args.Entries[index:]...)
			logger.PrettyDebug(logger.DInfo, "S%d: follower append [%v]", rf.me, args.Entries[index:])
			break
		}
	}
	// append entries rpc 5
	// The min in the final step (#5) of AppendEntries is necessary,
	// and it needs to be computed with the index of the last new entry.
	// It is not sufficient to simply have the function that applies things
	// from your log between lastApplied and commitIndex stop when it reaches the end of your log.
	// This is because you may have entries in your log that differ
	// from the leader’s log after the entries that the leader sent you (which all match the ones in your log).
	// Because you only truncate your log if you have conflicting entries, those won’t be removed,
	// and if leaderCommit is beyond the entries the leader sent you, you may apply incorrect entries.
	if args.LeaderCommit > rf.commitIndex {
		// why rf.log.lastLog().Index?
		// Because if appended entries at the above, the lastLog is exactly the last new entry
		rf.commitIndex = Min(args.LeaderCommit, rf.log.lastLog().Index)
		rf.apply()
	}
	reply.Success = true
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
		// rules for leader 3
		if lastLog.Index >= rf.nextIndex[peerID] || isHeartbeat {
			nextIndex := rf.nextIndex[peerID]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if lastLog.Index+1 < nextIndex {
				nextIndex = lastLog.Index
			}
			prevLog := rf.log.at(nextIndex - 1)
			// logger.PrettyDebug(logger.DLeader, "S%v: prevLog %#v", rf.me, prevLog)
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
		// rules for leader 3.1
		if reply.Success {
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[serverID] = Max(rf.nextIndex[serverID], next)
			rf.matchIndex[serverID] = Max(rf.matchIndex[serverID], match)
			// rf.matchIndex[serverID] = Max(rf.matchIndex[serverID], args.PrevLogIndex+len(args.Entries))
			// rf.nextIndex[serverID] = Max(rf.nextIndex[serverID], args.PrevLogIndex+len(args.Entries)+1)
			logger.PrettyDebug(logger.DLeader, "S%v: S%v append success next %v match %v", rf.me, serverID, rf.nextIndex[serverID], rf.matchIndex[serverID])
		} else if reply.Conflict {
			logger.PrettyDebug(logger.DLeader, "S%v: Conflict from %v %#v", rf.me, serverID, reply)
			if reply.Xterm == -1 {
				rf.nextIndex[serverID] = reply.Xlen
			} else {
				// Upon receiving a conflict response, the leader should first search its log for conflictTerm.
				// If it finds an entry in its log with that term,
				// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
				// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
				lastLogInXTerm := rf.findLastLogInTerm(reply.Xterm)
				logger.PrettyDebug(logger.DLeader, "S%v: lastLogInXTerm %v", rf.me, lastLogInXTerm)
				if lastLogInXTerm != -1 {
					rf.nextIndex[serverID] = lastLogInXTerm + 1 // todo
				} else {
					rf.nextIndex[serverID] = reply.Xindex
				}
			}
			logger.PrettyDebug(logger.DLeader, "S%v: after compute, leader nextIndex[%v] %v", rf.me, serverID, rf.nextIndex[serverID])
		} else if rf.nextIndex[serverID] > 1 {
			rf.nextIndex[serverID]--
			logger.PrettyDebug(logger.DLeader, "S%v: nextIndex[%v]--, becomes %v", rf.me, serverID, rf.nextIndex[serverID])
		}
		rf.leaderCommitRule()
	}
}

func (rf *Raft) leaderCommitRule() {
	// leader rule 4
	if rf.status != LEADER {
		return
	}

	for n := rf.commitIndex + 1; n <= rf.log.lastLog().Index; n++ {
		if rf.log.at(n).Term == rf.currentTerm {
			count := 0
			for i, _ := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers) / 2 {
				rf.commitIndex = n
				logger.PrettyDebug(logger.DLeader, "S%v: leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) findLastLogInTerm(term int) int {
	for i := rf.log.lastLog().Index; i > 0; i-- {
		t := rf.log.at(i).Term
		if t == term {
			return i
		} else if t < term {
			break
		}
	}
	return -1
}
