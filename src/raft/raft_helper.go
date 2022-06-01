package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

//set ticker period according to this peer state
func (rf *Raft) setTickerPeriod(state PeerState) {
	if rf.state == Leader {
		rf.tickerPeriod = time.Duration(AppendEntriesInterval) * time.Millisecond
	} else {
		rf.tickerPeriod = time.Duration(rand.Intn(ElectionTimeoutUB-ElectionTimeoutLB)+ElectionTimeoutLB) * time.Millisecond
	}
	rf.DPrintf("rf.setTickerPeriod", "set ticker period to %v", rf.tickerPeriod)
}

func (rf *Raft) getAllPeers() []int {
	receiverList := make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
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
		rf.DPrintf("rf.toLeader", "Fail to become a leader due to unmatched term")
	} else {
		log.Printf("****** server %v to leader state for term %v ******", rf.me, rf.currentTerm)
		rf.termLeader = rf.me
		rf.state = Leader
		rf.isolated = false
		rf.setVotedFor(rf.me)
		rf.setTickerPeriod(rf.state)
		success = true
	}
	return success
}

func (rf *Raft) toCandidate(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	log.Printf("++++++ server %v to candidate state ++++++", rf.me)
	rf.state = Candidate
	rf.isolated = true
	rf.setVotedFor(-1)
	rf.setTickerPeriod(rf.state)
}

func (rf *Raft) toFollower(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	log.Printf("------ server %v to follower state -------", rf.me)
	rf.state = Follower
	rf.isolated = true
	rf.setVotedFor(-1)
	rf.setTickerPeriod(rf.state)
}

func (rf *Raft) verifyAEReq(args *AppendEntriesArgs) bool {
	result := true
	if args.Term < rf.currentTerm {
		result = false
	}
	return result
}

//vote for others when:
// - Leader:
// 		1.The election term in the request is higher than itself
// - Candidate:
//		1.It is not running an election by itself(determined by whether it has set votedFor to itself)
// - Follower:
// 		1.The election term in the request is higher than itself
func (rf *Raft) validateElection(args *RequestVoteArgs) bool {
	result := true
	if rf.currentTerm >= args.Term {
		rf.DPrintf("rf.validateElection", "rejects to vote because term doesn't match(%v/%v)", rf.currentTerm, args.Term)
		result = false
	}

	// if rf.currentTerm < args.Term && rf.votedFor != -1 {
	// 	rf.DPrintf("rf.validateElection", "rejects to vote because already vote for %v at term %v", rf.votedFor, args.Term)
	// 	result = false
	// }
	return result
}

func (rf *Raft) DPrintf(funcName string, str string, a ...interface{}) {
	s := fmt.Sprintf(str, a...)
	DPrintf("{Server %v}[%v]: %v", rf.me, funcName, s)
}
