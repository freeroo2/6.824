package raft

import "sync"

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

type Log struct {
	sync.Mutex
	Entries []*Entry
}

func (log *Log) lastLog() *Entry {
	log.Lock()
	defer log.Unlock()
	length := len(log.Entries)
	if length <= 0 {
		return nil
	}
	return log.Entries[length-1]

}

func (log *Log) append(e ...*Entry) {
	log.Lock()
	defer log.Unlock()
	log.Entries = append(log.Entries, e...)
}

func (log *Log) at(index int) *Entry {
	log.Lock()
	defer log.Unlock()
	length := len(log.Entries)
	if index < 0 || index >= length {
		return nil
	}
	return log.Entries[index]
}

func (log *Log) slice(index int) []*Entry {
	log.Lock()
	defer log.Unlock()
	return log.Entries[index:]
}
