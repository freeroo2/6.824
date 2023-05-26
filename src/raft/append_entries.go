package raft

import (
	"6.824/logger"
)

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
	Term         int
	Success      bool
	Conflict     bool
	Xindex       int
	Xterm        int
	Xlen         int
	SnapshotMiss bool
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// for debug
	cmds := EntriesToString(args.Entries)
	logger.PrettyDebug(logger.DInfo, "S%d: (term %d) follower 收到 [%v] AppendEntries %v, prevIndex %v, prevTerm %v", rf.me, rf.currentTerm, args.LeaderId, cmds, args.PrevLogIndex, args.PrevLogTerm)

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
	// 自身的快照Index比发过来的prevLogIndex还大
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.SnapshotMiss = true
		reply.Xindex = rf.lastIncludeIndex
		return
	}
	// appendentries rpc 2
	prevLog := rf.at(args.PrevLogIndex)
	if rf.getLogLastIndex() < args.PrevLogIndex {
		reply.Conflict = true
		reply.Xindex = -1
		reply.Xterm = -1
		reply.Xlen = len(rf.log)
		logger.PrettyDebug(logger.DConflict, "S%v: here Conflict Xterm %v, Xindex %v, Xlen %v, rf.logs: %v", rf.me, reply.Xterm, reply.Xindex, reply.Xlen, rf.LogToString())
		return
	}
	logger.PrettyDebug(logger.DSnap, "S%v: prevLog %#v", rf.me, prevLog)
	if prevLog.Term != args.PrevLogTerm {
		reply.Conflict = true
		xterm := prevLog.Term
		// " If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs. "
		// If a follower does have prevLogIndex in its log, but the term does not match,
		// it should return conflictTerm = log[prevLogIndex].Term,
		// and then search its log for the first index whose entry has term equal to conflictTerm.
		// here, conflictTerm = reply.Xterm
		for xindex := args.PrevLogIndex; xindex > rf.lastIncludeIndex; xindex-- {
			if rf.at(xindex-1).Term != xterm {
				reply.Xindex = xindex
				break
			}
		}
		reply.Xterm = xterm
		reply.Xlen = len(rf.log)
		logger.PrettyDebug(logger.DConflict, "S%v: Conflict Xterm %v, Xindex %v, Xlen %v", rf.me, reply.Xterm, reply.Xindex, reply.Xlen)
		return
	}
	for index, entry := range args.Entries {
		// append entries rpc 3
		//logger.PrettyDebug(logger.DInfo, "S%d: index=%d, rf.log.lastLog().Index=%d, entry.Term=%d, rf.log.at(entry.Index).Term=%d", rf.me, index, rf.log.lastLog().Index, entry.Term, rf.log.at(entry.Index).Term)
		if entry.Index <= rf.getLogLastIndex() && entry.Term != rf.at(entry.Index).Term {
			rf.truncate(entry.Index)
			rf.persist()
			logger.PrettyDebug(logger.DInfo, "S%d: follower remove slice from Index %v", rf.me, index)
		}
		// append entries rpc 4
		if entry.Index > rf.getLogLastIndex() {
			rf.append(args.Entries[index:]...)
			rf.persist()
			// cmds := EntriesCmdToString(args.Entries[index:])
			logger.PrettyDebug(logger.DInfo, "S%d: follower append [%v]", rf.me, rf.LogToString())
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
		rf.commitIndex = Min(args.LeaderCommit, rf.getLogLastIndex())
		rf.apply()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderAppendEntries(isHeartbeat bool) {
	for peerID, _ := range rf.peers {
		if peerID == rf.me {
			rf.ResetElectionTimeOut()
			continue
		}
		// rules for leader 3
		if rf.getLogLastIndex() >= rf.nextIndex[peerID] || isHeartbeat {
			nextIndex := rf.nextIndex[peerID]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			logger.PrettyDebug(logger.DSnap, "S%v: -> S%v, next %v, rf.lastIncludeIndex %v", rf.me, peerID, nextIndex, rf.lastIncludeIndex)
			if nextIndex <= rf.lastIncludeIndex {
				logger.PrettyDebug(logger.DSnap, "S%v: enter leaderSendSnapshot to S%v", rf.me, peerID)
				go rf.leaderSendSnapshot(peerID)
				continue
			}
			if rf.getLogLastIndex()+1 < nextIndex {
				nextIndex = rf.getLogLastIndex()
			}
			prevLog := rf.at(nextIndex - 1)
			// logger.PrettyDebug(logger.DSnap, "S%v: -> S%v, next = %v", rf.me, peerID, nextIndex)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLog.Index,
				PrevLogTerm:  prevLog.Term,
				LeaderCommit: rf.commitIndex,
				Entries:      make([]*Entry, rf.getLogLastIndex()-nextIndex+1),
			}
			//
			if rf.lastIncludeIndex > 0 && prevLog.Index == 0 {
				// 只有哨兵entry
				args.PrevLogIndex = rf.lastIncludeIndex
				args.PrevLogTerm = rf.lastIncludeTerm
			}
			copy(args.Entries, rf.slice(nextIndex))
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
				if lastLogInXTerm > 0 {
					rf.nextIndex[serverID] = lastLogInXTerm + 1 // todo
				} else {
					rf.nextIndex[serverID] = reply.Xindex
				}
			}
			logger.PrettyDebug(logger.DLeader, "S%v: after compute, leader nextIndex[%v] %v", rf.me, serverID, rf.nextIndex[serverID])
		} else if reply.SnapshotMiss {
			rf.nextIndex[serverID] = reply.Xindex + 1
			logger.PrettyDebug(logger.DLeader, "S%v: nextIndex[%v]++, becomes %v", rf.me, serverID, rf.nextIndex[serverID])
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

	//todo
	index := Max(rf.commitIndex+1, rf.lastIncludeIndex+1)
	for n := index; n <= rf.getLogLastIndex(); n++ {
		if rf.at(n).Term == rf.currentTerm {
			count := 1
			for i, _ := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = n
				//logger.PrettyDebug(logger.DLeader, "S%v: leader尝试提交 index %v", rf.me, rf.commitIndex)
				rf.apply()
				break
			}
		}
	}
}

func (rf *Raft) findLastLogInTerm(term int) int {
	for i := rf.getLogLastIndex(); i > rf.lastIncludeIndex; i-- {
		t := rf.at(i).Term
		if t == term {
			return i
		} else if t < term {
			break
		}
	}
	return -1
}
