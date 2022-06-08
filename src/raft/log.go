package raft

import (
	"log"
	"sync"

	"6.824/labgob"
)

const LogIndexOffset = 1 //idx starts from 1

type Entry struct {
	Term    int
	Command interface{}
}

//define:
// logical size: length including a the past snapshoted entries
// valid size:  length of the entris in memory
// logical size >= valid size
type Log struct {
	mu             sync.Mutex
	savedIdx       int //idx of the last lg that has been saved(is the valid idx of the entries not the unzipped one)
	entries        []Entry
	snapshotOffset int
	dirty          bool
}

func makeLog(entries []Entry) Log {
	lg := Log{}
	lg.savedIdx = -1
	lg.entries = entries
	lg.snapshotOffset = 0
	lg.dirty = true
	return lg
}

//access lg at certain logical index, return duplicated value
func (lg *Log) at(idx int) Entry {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if idx < LogIndexOffset || idx >= lg.size()+LogIndexOffset {
		log.Panicf("Index %v is out of range of array with length=%v", idx, lg.size())
	}

	return lg.entries[idx-lg.snapshotOffset-LogIndexOffset]
}

//
func (lg *Log) set(idx int, val Entry) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if idx < LogIndexOffset || idx > lg.size()+LogIndexOffset { //start could be lg.size() + LogIndexOffset + 1, cuz it allows new entry to be appended
		log.Panicf("index is %v, but it must be greater than 1 and no greater than %v", idx, lg.size()+LogIndexOffset)
	}

	if idx == lg.size()+LogIndexOffset {
		lg.entries = append(lg.entries, val)
	} else {
		lg.entries[idx-lg.snapshotOffset-LogIndexOffset] = val
	}
	lg.dirty = true
}

func (lg *Log) getEntries(start int, end int) []Entry {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if start == end {
		return make([]Entry, 0)
	}
	if end > lg.size()+LogIndexOffset || end < LogIndexOffset || start < LogIndexOffset || start >= lg.size()+LogIndexOffset {
		log.Panicf("Index out of range, either start(%v) < 1 or end(%v) > %v", start, end, lg.nextIdx())
	}

	if start > end {
		log.Panic("Start index must be smaller than or equal to end index")
	}

	//end is exclusive
	entries := make([]Entry, end-start)
	copy(entries, lg.entries[start-lg.snapshotOffset-LogIndexOffset:end-lg.snapshotOffset-LogIndexOffset])
	// DPrintf("new entries in log:%v, from %v to %v", entries, start, end)
	return entries
}

//operating on logical index
func (lg *Log) setEntries(start int, newEntries []Entry) {
	for i := 0; i < len(newEntries); i++ {
		lg.set(start+i, newEntries[i])
	}
}

//The logical size
func (lg *Log) size() int {
	return len(lg.entries) + lg.snapshotOffset
}

//The logical nextIdx
func (lg *Log) nextIdx() int {
	return lg.size() + LogIndexOffset
}

//The logical lastIdx
func (lg *Log) latestIdx() int {
	return lg.size() - 1 + LogIndexOffset
}

func (lg *Log) encode(e *labgob.LabEncoder) *labgob.LabEncoder {
	//TODO
	return e
}

//return the head index of the given term
//if term is not found, return the index the given term should be inserted into
func (lg *Log) locateTermHead(term int) (int, bool) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	loc, found := binarySearch(lg.entries, term)
	return loc + lg.snapshotOffset + LogIndexOffset, found
}

func (lg *Log) locateTermTail(term int) (int, bool) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if _, found := binarySearch(lg.entries, term); found {
		tailNext, _ := binarySearch(lg.entries, term+1)
		return tailNext - 1 + LogIndexOffset, true
	} else {
		return -1 + LogIndexOffset, false
	}
}

func (lg *Log) containsLogOfTerm(term int) bool {
	_, found := binarySearch(lg.entries, term)
	return found
}
