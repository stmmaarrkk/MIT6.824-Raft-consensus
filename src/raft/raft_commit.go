package raft

import (
	"sort"
)

//return true if the there are commands applied
func (rf *Raft) applyCmdOnceReady(resCh chan bool) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	timeoutCount := 0
	normalCount := 1
	for res := range resCh {
		if !res {
			timeoutCount++
		} else {
			normalCount++
		}
		if timeoutCount > rf.nPeer/2 {
			return false
		} else if normalCount > rf.nPeer/2 {
			rf.applyCommands() //if its the leader's term, apply command, this line could be modified
			return true
		}
	}
	return true
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Leader should never commit the log not in its term
	if entry, err := rf.log.getLatestEntry(); err == nil {
		if entry.Term != rf.currentTerm {
			return
		}
	} else {
		rf.dPrintf("rf.updateCommitIndex", "Not enough log to be committed")
		return
	}

	old := rf.commitIndex
	matchIdxes := make([]int, rf.nPeer)
	copy(matchIdxes, rf.matchIndex)
	sort.Ints(matchIdxes)
	midIdx := rf.nPeer / 2
	rf.commitIndex = Max(rf.commitIndex, matchIdxes[midIdx])
	if old != rf.commitIndex {
		rf.dPrintf("rf.updateCommitIndex", "commitIndex updated from %v to %v", old, rf.commitIndex)
	}
	rf.matchIndexChanged = false
}
