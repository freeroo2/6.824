package raft

import "6.824/logger"

type InstallSnapshotArg struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArg, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	logger.PrettyDebug(logger.DSnap, "S%v: enter Snapshot", rf.me)
	// Your code here (2D).
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果上次snapshot的lastIncludeIndex比这次snapshot的index大，则直接返回
	if rf.lastIncludeIndex >= index {
		return
	}

	// 更新rf的日志，正确截断index及之前的log，保留index之后的log
	tlogs := make([]*Entry, 0)
	tlogs = append(tlogs, &Entry{-1, 0, 0})
	for i := index + 1; i <= rf.getLogLastIndex(); i++ {
		tlogs = append(tlogs, rf.at(i))
	}

	// 更新快照index和term
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.at(index).Term
	rf.log = tlogs
	rf.snapshot = snapshot

	// 更新rf的commitIndex和lastApplied
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArg, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// rules for servers
	// all servers 2
	reply.Term = rf.currentTerm
	// append entries rpc 1
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.setNewTerm(args.Term)
	}
	rf.ResetElectionTimeOut()
	// Candidates 3
	if rf.status == CANDIDATE {
		rf.status = FOLLOWER
	}

	if args.LastIncludedIndex <= rf.lastIncludeIndex || rf.commitIndex > args.LastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	tlogs := make([]*Entry, 0)
	tlogs = append(tlogs, &Entry{-1, 0, 0})
	index := args.LastIncludedIndex
	for i := index + 1; i <= rf.getLogLastIndex(); i++ {
		tlogs = append(tlogs, rf.at(i))
	}

	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.commitIndex = rf.lastIncludeIndex
	rf.lastApplied = rf.lastIncludeIndex

	rf.log = tlogs
	rf.snapshot = args.Data

	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- applyMsg
}

func (rf *Raft) leaderSendSnapshot(serverID int) {
	rf.mu.Lock()
	args := InstallSnapshotArg{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludeIndex,
		LastIncludedTerm:  rf.lastIncludeTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(serverID, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	if rf.status != LEADER || rf.currentTerm != args.Term {
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm {
		rf.setNewTerm(reply.Term)
		rf.mu.Unlock()
		return
	}
	if reply.Term == rf.currentTerm {
		rf.matchIndex[serverID] = args.LastIncludedIndex
		rf.nextIndex[serverID] = args.LastIncludedIndex + 1
	}
	rf.mu.Unlock()
}
