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
	rf.dPrintf("rf.setTickerPeriod", "set ticker period to %v", rf.tickerPeriod)
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
		rf.dPrintf("rf.toLeader", "Fail to become a leader due to unmatched term")
	} else {
		rf.logPrintf("****** to leader status for term %v ******", rf.currentTerm)
		rf.status = Leader
		rf.isolated = false
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
	rf.logPrintf("++++++ to candidate status ++++++")
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
	rf.logPrintf("------ to follower status -------")
	rf.status = Follower
	rf.isolated = true
	rf.setVotedFor(-1)
	rf.setTickerPeriod(rf.status)
}
func (rf *Raft) verifyLeader(args *AppendEntriesArgs) bool {
	result := true
	if rf.currentTerm > args.Term {
		result = false
	}

	if rf.commitIndex > args.LeaderCommit {
		result = false
	}

	return result
}

func (rf *Raft) verifyAEReq(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	result := true

	if args.PrevLogIndex > rf.log.latestIdx() { //start index exceeds the size of log
		rf.dPrintf("rf.verifyAEReq", "AE verification failed: not enough log entries on follower(only has %v)", rf.log.size())
		result = false
		reply.XLen = args.PrevLogIndex - rf.log.latestIdx()
	} else if args.PrevLogIndex > 0 { //this block will be executed if there is previous entry in the log
		if rf.log.at(args.PrevLogIndex).Term != args.PrevLogTerm { //term doesn't match
			rf.dPrintf("rf.verifyAEReq", "AE verification failed: latest log term doesn't match, ours is %v, but %v is expected", rf.log.at(args.PrevLogIndex).Term, args.PrevLogTerm)
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
func (rf *Raft) verifyVoteReq(args *RequestVoteArgs, reply *RequestVoteReply) bool {
	result := true
	reason := ""
	if rf.currentTerm >= args.Term {
		reason = fmt.Sprintf("my term(%v) is greater than that of the candidate(%v)", rf.currentTerm, args.Term)
		result = false
	} else if myLatestLog := rf.log.getLatestEntry(); myLatestLog != nil {
		//If the logs have last entries with different terms, then the log with the later term is more up-to-date
		if myLatestLog.Term > args.LastLogTerm {
			reason = fmt.Sprintf("the term in my latest log(%v) is greater than that of the candidate(%v)", myLatestLog.Term, args.LastLogTerm)
			result = false
		}

		//Note that sometimes their latest log could have same term
		//(when they follow the same leader, and the leader is dead, one of the follower turns into a candidate)

		//If the logs end with the same term, then whichever log is longer is more up-to-date
		if myLatestLog.Term == args.LastLogTerm && rf.log.latestIdx() > args.LastLogIndex {
			reason = fmt.Sprintf("my LatestLogIdx(%v) is greater than that of the candidate(%v)", rf.log.latestIdx(), args.LastLogIndex)
			result = false
		}
	}

	if !result {
		rf.logPrintf("Rejects to vote for %v: %v", args.CandidateId, reason)
	}

	return result
}

func (rf *Raft) initRVReply(reply *RequestVoteReply) {
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

func (rf *Raft) updateCommitIndex() {
	old := rf.commitIndex
	matchIdxes := make([]int, rf.nPeer)
	copy(matchIdxes, rf.matchIndex)
	sort.Ints(matchIdxes)
	midIdx := rf.nPeer / 2
	rf.commitIndex = Max(rf.commitIndex, matchIdxes[midIdx])
	if old != rf.commitIndex {
		rf.dPrintf("rf.updateCommitIndex", "commitIndex updated from %v to %v", old, rf.commitIndex)
	}
	rf.applyCommands()
}

//Note that this function should generate request regardless of the fact that this request is hearbeat or not
// or the make-up req will not be sent until next Non-Heartbeat req
func (rf *Raft) generateAEReq(peerId int, key int) AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.log.at(prevLogIndex).Term
	}

	newEntries := make([]Entry, 0)

	//generate one non-heartbeat AE
	// rf.dPrintf("rf.geneateAE", "next:%v, match%v", rf.nextIndex[peerId], rf.matchIndex[rf.me])
	if rf.nextIndex[peerId] <= rf.matchIndex[rf.me] {
		newEntries = rf.log.getEntries(rf.nextIndex[peerId], rf.matchIndex[rf.me]+1)
		rf.dPrintf("rf.generateAE", "For peer %v, %v entries generated from %v ", peerId, len(newEntries), rf.nextIndex[peerId])
	} else {
		rf.dPrintf("rf.generateAE", "For peer %v, generate empty AE from %v", peerId, rf.nextIndex[peerId])
	}

	return AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      newEntries,
		LeaderCommit: rf.commitIndex,
		Key:          key,
	}
}

func (rf *Raft) initAEReply(reply *AppendEntriesReply) {
	reply.Id = rf.me
	reply.Term = rf.currentTerm //if this follower has higher term, should notify the leader
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = 0
}

func (rf *Raft) handleAEReply(peerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != Leader { //only leader handle reply
		rf.dPrintf("rf.handleAEReply", "No longer a Leader, ignore the reply...")
		return
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.matchIndex[peerId] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
			rf.commitIdxChanged = true
		}

		rf.dPrintf("rf.handleAEReply", "AE request(%v) approved by server %v", args.Key, peerId)
	} else {
		if reply.Term > rf.currentTerm { //the follower has a higher term
			rf.logPrintf("Leader's term is updated from %v to %v by follower %v", rf.currentTerm, reply.Term, peerId)
			rf.setCurrentTerm(reply.Term)
		} else if reply.XTerm == -1 { //not enough logs in follower
			rf.nextIndex[peerId] = args.PrevLogIndex - reply.XLen + 1
		} else if termHead, ok := rf.log.locateTermHead(reply.XTerm); ok { //the xTerm is in leader's log
			rf.nextIndex[peerId] = termHead
			rf.dPrintf("rf.handleAEReply", "nextIndex[%v] is set to %v(term head)", peerId, termHead)
		} else { //xTerm not in leader's log
			rf.nextIndex[peerId] = reply.XIndex
			rf.dPrintf("rf.handleAEReply", "nextIndex[%v] is set to %v(xIndex)", peerId, termHead)
		}

		if rf.nextIndex[peerId] <= 0 {
			log.Fatalf("nextIndex[%v] should never be set zero!!!! check the reply:%v", peerId, reply)
		}

		rf.dPrintf("rf.handleAEReply", "AE request(%v) denied by server %v, reply:{xT:%v, xI:%v, xL:%v}", args.Key, peerId, reply.XTerm, reply.XIndex, reply.XLen)
	}
}

func (rf *Raft) createLog(command interface{}) Entry {
	return Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
}

func (rf *Raft) dPrintf(funcName string, str string, a ...interface{}) {
	if !rf.killed() {
		s := fmt.Sprintf(str, a...)
		DPrintf("{%v-%v}[%v]: %v", statusToStr(rf.status), rf.me, funcName, s)
	}
}

func (rf *Raft) logPrintf(str string, a ...interface{}) {
	if !rf.killed() {
		s := fmt.Sprintf(str, a...)
		log.Printf("{%v-%v}: %v", statusToStr(rf.status), rf.me, s)
	}
}
