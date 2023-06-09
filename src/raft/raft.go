package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/logger"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type PeerStatus int

const (
	FOLLOWER  PeerStatus = 0
	CANDIDATE PeerStatus = 1
	LEADER    PeerStatus = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status           PeerStatus
	heartBeat        time.Duration
	electionTimeOut  time.Time
	currentTerm      int
	votedFor         int
	log              []*Entry // first index is 1
	commitIndex      int      // initialized to 0
	lastApplied      int      // initialized to 0
	nextIndex        []int    // initialized to leader last log index + 1
	matchIndex       []int    // initialized to 0
	applyCh          chan ApplyMsg
	applyCond        *sync.Cond
	lastIncludeIndex int
	lastIncludeTerm  int
	snapshot         []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.status == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	logger.PrettyDebug(logger.DPersist, "S%d: log to persist: %v", rf.me, rf.LogToString())
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []*Entry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		// error...
		logger.PrettyDebug(logger.DError, "S%v: readPersist error", rf.me)

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == LEADER {
		index = rf.getLogLastIndex() + 1
		term = rf.currentTerm
		isLeader = true
		entry := &Entry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.append(entry)
		rf.persist()
		logger.PrettyDebug(logger.DLeader, "S%v: leader append entries %#v", rf.me, entry)
		rf.leaderAppendEntries(false)
		return index, term, isLeader
	}
	term = rf.currentTerm
	isLeader = false
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ResetElectionTimeOut() {
	rf.electionTimeOut = time.Now().Add(time.Duration((GetRand(int64(rf.me)) + 150)) * time.Millisecond)
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// 如果是leader，就发送心跳消息；如果不是leader并超过timeout，就成为候选人发起选举
		// election timeout set to 200ms~400ms
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.status == LEADER {
			rf.leaderAppendEntries(true)
		}
		if time.Now().After(rf.electionTimeOut) {
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which  the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	logger.PrettyDebug(logger.DInfo, "S%d: enter Make", rf.me)
	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()
	rf.log = make([]*Entry, 0)
	rf.append(&Entry{
		Command: -1,
		Term:    0,
		Index:   0,
	})
	rf.status = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0                         // initialized to 0
	rf.lastApplied = 0                         // initialized to 0
	rf.nextIndex = make([]int, len(rf.peers))  // initialized to leader last log index + 1
	rf.matchIndex = make([]int, len(rf.peers)) // initialized to 0
	rf.applyCh = applyCh
	rf.heartBeat = 100 * time.Millisecond
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.ResetElectionTimeOut()
	rf.mu.Unlock()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	logger.PrettyDebug(logger.DPersist, "S%d: log from readPersist: %v", rf.me, rf.LogToString())
	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	// logger.PrettyDebug(logger.DLog, "S%d, after make", rf.me)
	go rf.applier()
	return rf
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	//logger.PrettyDebug(logger.DCommit, "S%d, rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// all servers rule 1
	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied && rf.getLogLastIndex() > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.at(rf.lastApplied).Command,
			}
			// logger.PrettyDebug(logger.DCommit, "S%d, COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			//logger.PrettyDebug(logger.DCommit, "S%d, rf.applyCond.Wait()", rf.me)
		}
	}
}

// todo
func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.at(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}
