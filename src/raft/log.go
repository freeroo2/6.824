package raft

import (
	"fmt"
	"strings"
)

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

func (rf *Raft) lastLog() *Entry {
	length := len(rf.log)
	if length <= 0 {
		return nil
	}
	return rf.log[length-1]

}

func (rf *Raft) getLogLastIndex() int {
	return len(rf.log) - 1 + rf.lastIncludeIndex
}

func (rf *Raft) getLogLastTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastIncludeTerm
	}
	return rf.lastLog().Term
}

func (rf *Raft) append(e ...*Entry) {
	rf.log = append(rf.log, e...)
}

// calculate with lastIncludeIndex
func (rf *Raft) at(index int) *Entry {
	calIndex := index - rf.lastIncludeIndex
	length := len(rf.log)
	if calIndex < 0 || calIndex >= length {
		return nil
	}
	return rf.log[calIndex]
}

func (rf *Raft) slice(index int) []*Entry {
	calIndex := index - rf.lastIncludeIndex
	length := len(rf.log)
	if calIndex < 0 || calIndex >= length {
		return nil
	}
	return rf.log[calIndex:]
}

func (rf *Raft) truncate(index int) {
	calIndex := index - rf.lastIncludeIndex
	if calIndex <= 0 || calIndex >= len(rf.log) {
		return
	}
	rf.log = rf.log[:calIndex]
}

func (e *Entry) String() string {
	return fmt.Sprint(e.Term)
}

func (rf *Raft) LogToString() string {
	nums := []string{}
	for _, entry := range rf.log {
		nums = append(nums, fmt.Sprintf("%4d", entry.Index))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}
