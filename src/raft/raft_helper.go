package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sort"
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
		log.Printf("****** server %v to leader status for term %v ******", rf.me, rf.currentTerm)
		rf.termLeader = rf.me
		rf.status = Leader
		rf.isolated = false
		rf.aERound = 0
		rf.aEVerification = rand.Intn(10000000)
		rf.nextIndex = make([]int, rf.nPeer)
		for i, _ := range rf.nextIndex {
			rf.nextIndex[i] = rf.log.nextIdx()
		}

		// //empty command buffer( but might not want to empty it)
		// for len(rf.commandBuffer) > 0 {
		// 	<-rf.commandBuffer
		// }

		rf.matchIndex = make([]int, rf.nPeer) //default value is 0
		rf.setVotedFor(rf.me)
		rf.setTickerPeriod(rf.status)
		success = true
	}
	return success
}

func (rf *Raft) toCandidate(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	log.Printf("++++++ server %v to candidate status ++++++", rf.me)
	rf.status = Candidate
	rf.isolated = true
	rf.setVotedFor(-1)
	rf.setTickerPeriod(rf.status)
}

func (rf *Raft) toFollower(lock *sync.Mutex) {
	if lock != nil {
		lock.Lock()
		defer lock.Unlock()
	}
	log.Printf("------ server %v to follower status -------", rf.me)
	rf.status = Follower
	rf.isolated = true
	rf.setVotedFor(-1)
	rf.setTickerPeriod(rf.status)
}

func (rf *Raft) verifyAEReq(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	result := true
	if args.Term < rf.currentTerm {
		result = false
	}

	if args.PrevLogIndex > rf.log.latestIdx() { //start index exceeds the size of log
		rf.DPrintf("rf.verifyAEReq", "AE verification failed: not enough log entries on follower(only has %v)", rf.log.size())
		result = false
		reply.XLen = args.PrevLogIndex - rf.log.latestIdx()
	} else if args.PrevLogIndex > 0 { //this block will be executed if there is previous entry in the log
		if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm { //term doesn't match
			rf.DPrintf("rf.verifyAEReq", "AE verification failed: latest log term doesn't match, ours is %v, but %v is expected", rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
			result = false
			reply.XTerm = rf.log.at(args.PrevLogIndex).Term
			loc, _ := rf.log.locateTermHead(rf.log.at(args.PrevLogIndex).Term)
			reply.XIndex = loc
		}
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

func (rf *Raft) updateCommitIndex() {
	old := rf.commitIndex
	matchIdxes := make([]int, rf.nPeer)
	copy(matchIdxes, rf.matchIndex)
	sort.Ints(matchIdxes)
	midIdx := rf.nPeer / 2
	rf.commitIndex = Max(rf.commitIndex, matchIdxes[midIdx])
	rf.DPrintf("rf.updateCommitIndex", "commitIndex updated from %v to %v", old, rf.commitIndex)
	rf.applyCommands()
}

func (rf *Raft) DPrintf(funcName string, str string, a ...interface{}) {
	s := fmt.Sprintf(str, a...)
	DPrintf("{Server %v}[%v]: %v", rf.me, funcName, s)
}

func (rf *Raft) generateAE(peerId int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.log.at(prevLogIndex).Term
	}

	newEntries := make([]Entry, 0)
	//generate one non-heartbeat AE
	if (rf.aERound%NonHeartbeatAEInterval == 0 || rf.aERound == 1) && rf.nextIndex[peerId] < rf.log.nextIdx() {
		rf.DPrintf("rf.generateAE", "For peer %v, entries generated from %v to %v", peerId, rf.nextIndex[peerId], rf.log.latestIdx())
		newEntries = rf.log.getEntries(rf.nextIndex[peerId], rf.log.nextIdx())

	}
	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.termLeader,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      newEntries,
		LeaderCommit: rf.commitIndex,
		Verification: rf.aEVerification,
	}
}

func (rf *Raft) handleAEReply(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader { //only leader handle reply
		return
	}

	if rf.aEVerification != args.Verification {
		rf.DPrintf("rf.handleAEReply", "AE request for %v is expired", peerId)
	} else if reply.Success {
		rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1

		rf.DPrintf("rf.handleAEReply", "AE request approved by server %v", peerId)
	} else if reply.Term > rf.currentTerm { //the follower has a higher term
		rf.DPrintf("rf.handleAEReply", "AE request denied by server %v because higher term(%v) is found", peerId, reply.Term)
	} else {
		if reply.XTerm == -1 { //not enough logs in follower

			rf.nextIndex[peerId] = args.PrevLogIndex - reply.XLen + 1
		} else if termHead, ok := rf.log.locateTermHead(reply.XTerm); ok { //the xTerm is in leader's log
			DPrintf("termHead")
			rf.nextIndex[peerId] = termHead
		} else { //xTerm not in leader's log
			DPrintf("index")
			rf.nextIndex[peerId] = reply.XIndex
		}
		if rf.nextIndex[peerId] == 0 {
			DPrintf("reply: %v", reply)
			log.Fatalf("next Index of %v become zero!!!!", peerId)
		}
		rf.DPrintf("rf.handleAEReply", "AE request denied by server %v", peerId)
	}
}

func (rf *Raft) createLog(command interface{}) Entry {
	return Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
}
