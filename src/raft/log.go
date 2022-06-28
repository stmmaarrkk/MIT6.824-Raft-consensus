package raft

import (
	"errors"
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
	snapshotOffset int //starts from 0
	dirty          bool
	tail           int //physical tail, point to the last entry
}

func (lg *Log) init() {
	lg.savedIdx = -1
	lg.entries = make([]Entry, 0)
	lg.snapshotOffset = 0
	lg.dirty = true
	lg.tail = -1 //the physical index
}
func makeLog() *Log {
	lg := Log{}
	lg.init()
	return &lg
}

//access lg at certain logical index, return duplicated value
func (lg *Log) at(idx int) Entry {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if idx < LogIndexOffset || idx > lg.latestIdx() {
		log.Panicf("Index %v is out of range of array with length=%v", idx, lg.size())
	}

	return lg.entries[lg.toPIdx(idx)]
}

func (lg *Log) extend(entries ...Entry) *Log {
	lg.setEntries(lg.nextIdx(), entries...)
	return lg
}

//return the copy of the slice which contains the entries in between index [start, end)
func (lg *Log) getEntries(start int, end int) []Entry {
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
	if end > start {
		copy(entries, lg.entries[lg.toPIdx(start):lg.toPIdx(end)])
	}
	// DPrintf("new entries in log:%v, from %v to %v", entries, start, end)
	return entries
}

func (lg *Log) getAllEntries() []Entry {
	return lg.getEntries(LogIndexOffset, lg.nextIdx())
}

//CAREFUL!! DO NOT TRY TO MODIFY RETURN ENTRY
func (lg *Log) getLatestEntry() (Entry, error) {

	//non empty log
	if !lg.isEmpty() {
		return lg.at(lg.latestIdx()), nil
	} else {
		return Entry{}, errors.New("fail to retrieve latest entry due to empty log")
	}
}

//operating on logical index
func (lg *Log) setEntries(start int, newEntries ...Entry) {
	lg.mu.Lock()
	defer lg.mu.Unlock()

	if start < LogIndexOffset || start > lg.nextIdx() { //start could be lg.size() + LogIndexOffset + 1, cuz it allows new entry to be appended
		log.Panicf("The given start index is %v, but it must be greater than 1 and no greater than %v", start, lg.nextIdx())
	}

	for i, entry := range newEntries {
		lIdx := start + i
		pIdx := lg.toPIdx(lIdx)

		if pIdx == len(lg.entries) {
			lg.entries = append(lg.entries, Entry{})
		} else if pIdx > len(lg.entries) {
			log.Panicf("Unexpected error happens: pIdx=%v, the physical len of entries is %v", pIdx, len(lg.entries))
		}

		lg.tail = Max(pIdx, lg.tail)
		lg.entries[pIdx] = entry
		lg.dirty = true
	}
}

//entry after start(start is included) will be removed
func (lg *Log) deleteAfter(start int) {
	if start < LogIndexOffset {
		log.Fatalf("delete start(%v) is too small", start)
	}
	if start <= lg.snapshotOffset {
		log.Fatalf("log.deleteAfter has not been implement yet")
	}
	if start <= lg.latestIdx() {
		lg.tail = start - 1
	}
}

//convert physical idx to logical idx
func (lg *Log) toLIdx(pIdx int) int {
	return pIdx + lg.snapshotOffset + LogIndexOffset
}

//convert logical idx to physical idx
func (lg *Log) toPIdx(lIdx int) int {
	return lIdx - LogIndexOffset - lg.snapshotOffset
}

//The logical size
func (lg *Log) size() int {
	return lg.latestIdx() - LogIndexOffset + 1
}

//The logical nextIdx
func (lg *Log) nextIdx() int {
	return lg.latestIdx() + 1
}

//The logical lastIdx
func (lg *Log) latestIdx() int {
	return lg.toLIdx(lg.tail)
}

//return if logical log is empty or not
func (lg *Log) isEmpty() bool {
	return lg.size() == 0
}

func (lg *Log) encode(e *labgob.LabEncoder) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	if err := e.Encode(lg.snapshotOffset); err != nil {
		return err
	}
	if err := e.Encode(lg.entries); err != nil {
		return err
	}

	return nil
}

func (lg *Log) decode(d *labgob.LabDecoder) error {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	lg.init() //to the initial state
	if err := d.Decode(&lg.snapshotOffset); err != nil {
		return err
	}

	if err := d.Decode(&lg.entries); err != nil {
		return err
	}
	lg.tail = len(lg.entries) - 1

	return nil
}

//return the head index of the given term
//if term is not found, return the index the given term should be inserted into
func (lg *Log) locateTermHead(term int) (int, bool) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	loc, found := binarySearch(lg.entries, term)
	return lg.toLIdx(loc), found
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
