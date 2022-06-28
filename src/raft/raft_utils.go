package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

//set ticker period according to this peer status
func (rf *Raft) setTickerPeriod(status PeerStatus) {
	if rf.status == Leader {
		rf.tickerPeriod = time.Duration(AppendEntriesInterval) * time.Millisecond
	} else {
		rf.tickerPeriod = time.Duration(rand.Intn(ElectionTimeoutUB-ElectionTimeoutLB)+ElectionTimeoutLB) * time.Millisecond
	}
	rf.dPrintf("rf.setTickerPeriod", "set ticker period to %v", rf.tickerPeriod)
}

func (rf *Raft) getAllPeers() []int {
	receiverList := make([]int, rf.nPeer)
	for i := 0; i < rf.nPeer; i++ {
		receiverList[i] = i
	}

	return receiverList
}

func (rf *Raft) toLeader(elecTerm int, lock *sync.Mutex) bool {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	success := false
	if elecTerm != rf.currentTerm {
		rf.dPrintf("rf.toLeader", "Fail to become a leader due to unmatched term")
	} else {
		rf.status = Leader
		rf.aERound = 0
		rf.nextIndex = make([]int, rf.nPeer)
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = rf.log.nextIdx()
		}

		// //empty command buffer( but might not want to empty it)
		// for len(rf.commandBuffer) > 0 {
		// 	<-rf.commandBuffer
		// }

		rf.matchIndex = make([]int, rf.nPeer) //default value is 0
		rf.matchIndex[rf.me] = rf.log.latestIdx()
		rf.setTickerPeriod(rf.status)
		rf.resetTimerCh <- true
		success = true
		rf.logPrintf("****** to leader status for term %v ******", rf.currentTerm)
	}
	return success
}

func (rf *Raft) toCandidate(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	rf.status = Candidate
	rf.nextElectionTerm = rf.currentTerm + 1
	rf.setTickerPeriod(rf.status)
	rf.resetTimerCh <- true
	rf.logPrintf("++++++ to candidate status ++++++")
}

func (rf *Raft) toFollower(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	rf.status = Follower
	rf.setTickerPeriod(rf.status)
	rf.resetTimerCh <- true
	rf.logPrintf("------ to follower status -------")
}

func (rf *Raft) createLog(command interface{}) Entry {
	return Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
}

func (rf *Raft) toNewTerm(term int) {
	rf.setCurrentTerm(term)
	rf.setVotedFor(-1)
}

//getter and setter for persistent states
func (rf *Raft) setCurrentTerm(currentTerm int) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.currentTerm = currentTerm
	rf.persist()
	return true //return false if there is an error
}

func (rf *Raft) setVotedFor(votedFor int) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.votedFor = votedFor
	rf.persist()
	return true //return false if there is an error
}

//set entries, using logical index
//will delete the entries after the inserted one
func (rf *Raft) setLogs(start int, logs ...Entry) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	//remember to save to disk
	if len(logs) > 0 {
		rf.dPrintf("rf.setLogs", "insert log with length of %v to position %v ", len(logs), start)
		rf.log.setEntries(start, logs...)
		rf.log.deleteAfter(start + len(logs))
		rf.dPrintf("rf.setLogs", "the log: %v", rf.log.getEntries(1, rf.log.nextIdx()))
	}
	rf.persist()
	return true //return true if there is no error, for future use
}

//append entries, using logical index, only called by leader
func (rf *Raft) appendLogs(logs ...Entry) bool {
	if rf.status != Leader {
		log.Fatalf("In server %v: Only a leader can call rf.append", rf.me)
	}
	return rf.setLogs(rf.log.nextIdx(), logs...) //return true if there is no error, for future use
}

/*debug*/
func (rf *Raft) dPrintf(funcName string, str string, a ...interface{}) {
	if !rf.killed() {
		s := fmt.Sprintf(str, a...)
		DPrintf("{%v-%v(%v)}[%v]: %v", statusToStr(rf.status), rf.me, rf.currentTerm, funcName, s)
	}
}

func (rf *Raft) logPrintf(str string, a ...interface{}) {
	if !rf.killed() {
		s := fmt.Sprintf(str, a...)
		log.Printf("{%v-%v(%v)}: %v", statusToStr(rf.status), rf.me, rf.currentTerm, s)
	}
}

func (rf *Raft) logFatalf(str string, a ...interface{}) {
	if !rf.killed() {
		s := fmt.Sprintf(str, a...)
		log.Fatalf("{%v-%v(%v)}: %v", statusToStr(rf.status), rf.me, rf.currentTerm, s)
	}
}
