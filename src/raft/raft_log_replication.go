package raft

import (
	"log"
)

/*for append entry*/
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Id      int
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

//It's better if we wrap the code relavent to sending requests in a mutex,
// or sometimes only part of the server receive the new commit index which result in commit record consistency
func (rf *Raft) sendAppendEntries() {
	rf.mu.Lock()

	resCh := make(chan bool, rf.nPeer-1)

	rf.aERound++
	curRound := rf.aERound

	//send non heartbeat entries
	if rf.aERound%NonHeartbeatAEInterval == 0 {
		if rf.log.latestIdx() != rf.matchIndex[rf.me] {
			rf.matchIndexChanged = true
			rf.matchIndex[rf.me] = rf.log.latestIdx()
		}
		rf.dPrintf("rf.sendAppendEntries", "Sending Non-trivial AE reqs(%v)", curRound)
	} else {
		rf.dPrintf("rf.sendAppendEntries", "Sending Heartbeat AE reqs(%v)", curRound)
	}

	latestLogIdx := rf.matchIndex[rf.me] //important to create this variable to store the latest log idx, since accessing log is not thread safe

	receiverList := rf.getAllPeers()
	for r := range receiverList {
		if rf.status != Leader { //include this statement or sometimes fail the test
			return
		}
		if r != rf.me {
			args := rf.generateAEReq(r, latestLogIdx) //will generate heartbeat AE automatically
			go rf.sendAEReqTo(r, args, resCh)
		}
	}
	rf.mu.Unlock()

	rf.applyCmdOnceReady(resCh)
}

func (rf *Raft) sendAEReqTo(peerId int, args AppendEntriesArgs, resCh chan bool) {
	reply := AppendEntriesReply{}

	//check if the request has been successfully sent
	if rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply) {
		resCh <- true
		rf.handleAEReply(peerId, &args, &reply) //handle the reply
	} else if rf.status == Leader {
		resCh <- false
		rf.dPrintf("rf.sendAppendEntries", "fail to send AE to %v due to lost of connection", peerId)
	}

	//update commit index takes time, only executed when necessary
	if rf.matchIndexChanged {
		rf.updateCommitIndex()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.dPrintf("rf.AppendEntries", "An AE request comes from server %v of term %v", args.LeaderId, args.Term)

	rf.initAEReply(reply)

	if args.Term >= rf.currentTerm {
		if rf.status != Follower { //update the server's term when there is a higher term
			rf.toFollower(nil)
		}
		if args.Term > rf.currentTerm {
			rf.toNewTerm(args.Term)
		}
	} else { //ignore the request when the AE comes from lower term leader
		return
	}

	rf.resetTimerCh <- true
	if rf.verifyAEReq(args, reply) { //will generate the reply if fail to verify this AE req
		//the order of following part does matter!!
		// if rf.status != Follower { //If this server accept the AE request from other, means that other is more up-to-date
		// 	rf.toFollower(nil)
		// }
		reply.Success = rf.setLogs(args.PrevLogIndex+1, args.Entries...)
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, rf.log.latestIdx())
			rf.applyCommands()
		}

		if len(args.Entries) > 0 {
			rf.dPrintf("rf.AppendEntries", "%v entries are appended, %v entries in log", len(args.Entries), rf.log.size())
		}
	}
}

func (rf *Raft) verifyLeader(args *AppendEntriesArgs) bool {
	result := true
	if rf.currentTerm > args.Term {
		rf.dPrintf("rf.verifyLeader", "%v is not a legitimated leader due to lower term(%v)", args.LeaderId, args.Term)
		result = false
	}
	if rf.status == Leader { //There is only one leader in a term
		rf.logPrintf("server %v claims it is a leader and sent AE request to me, deny it!", args.LeaderId)
		result = false
	}

	// if rf.commitIndex > args.LeaderCommit {
	// 	rf.dPrintf("rf.verifyLeader", "%v is not a legitimated leader due to lower commitIdx(%v)", args.LeaderId, args.LeaderCommit)
	// 	result = false
	// }

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

//Note that this function should generate request regardless of the fact that this request is hearbeat or not
// or the make-up req will not be sent until next Non-Heartbeat req
func (rf *Raft) generateAEReq(peerId int, curLogIdx int) AppendEntriesArgs {
	prevLogIndex := rf.nextIndex[peerId] - 1
	prevLogTerm := 0
	if prevLogIndex > 0 {
		prevLogTerm = rf.log.at(prevLogIndex).Term
	}

	newEntries := make([]Entry, 0)

	//generate one non-heartbeat AE
	if rf.nextIndex[peerId] <= curLogIdx {
		newEntries = rf.log.getEntries(rf.nextIndex[peerId], curLogIdx+1)
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
			rf.matchIndexChanged = true
		}

		rf.dPrintf("rf.handleAEReply", "AE request approved by server %v", peerId)
	} else {
		if reply.Term > rf.currentTerm { //the follower has a higher term
			// rf.logPrintf("Leader's term is updated from %v to %v by follower %v", rf.currentTerm, reply.Term, peerId)
			rf.logPrintf("Leader's term is smaller than the follower's term")
			rf.toNewTerm(reply.Term)
			rf.toFollower(nil)
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

		rf.dPrintf("rf.handleAEReply", "AE request denied by server %v, reply:{xT:%v, xI:%v, xL:%v}", peerId, reply.XTerm, reply.XIndex, reply.XLen)
	}
}
