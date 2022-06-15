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
	Key          int //used for checking if the AE reply debugging
}
type AppendEntriesReply struct {
	Id      int
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) sendAppendEntries() {
	rf.aERound++
	curRound := rf.aERound

	//send non heartbeat entries
	if rf.aERound%NonHeartbeatAEInterval == 0 {
		rf.matchIndex[rf.me] = rf.log.latestIdx()
		rf.dPrintf("rf.sendAppendEntries", "Sending Non-trivial AE reqs(%v)", curRound)
	} else {
		rf.dPrintf("rf.sendAppendEntries", "Sending Heartbeat AE reqs(%v)", curRound)
	}

	receiverList := rf.getAllPeers()

	//update commit index takes time, only executed when necessary
	if rf.commitIdxChanged {
		rf.updateCommitIndex()
	}

	for r := range receiverList {
		if r != rf.me {
			go func(peerId int) {
				args := rf.generateAEReq(peerId, curRound) //will generate heartbeat AE automatically
				reply := AppendEntriesReply{}

				//check if the request has been successfully sent
				if rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply) {
					rf.handleAEReply(peerId, &args, &reply) //handle the reply
				} else if rf.status == Leader {
					rf.dPrintf("rf.sendAppendEntries", "fail to send AE(%v) to %v due to lost of connection", curRound, peerId)
				}
			}(r)
		}
	}
	// rf.DPrintf("rf.sendAppendEntries", "%v of %v AE requests are approved", len(successReplies), rf.nPeer-1)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.dPrintf("rf.AppendEntries", "An AE request comes from server %v of term %v", args.LeaderId, args.Term)

	rf.initAEReply(reply)
	if rf.verifyLeader(args) {
		// rf.dPrintf("rf.AppendEntries", "Timer", args.LeaderId)
		rf.isolated = false
		if rf.verifyAEReq(args, reply) { //will generate the reply if fail to verify this AE req
			//the order of following part does matter!!
			if rf.status != Follower { //If this server accept the AE request from other, means that other is more up-to-date
				rf.toFollower(nil)
			}

			if args.Term > rf.currentTerm {
				rf.resetTerm(args.Term)
			}
			reply.Success = rf.setLogs(args.PrevLogIndex+1, args.Entries...)
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = Min(args.LeaderCommit, rf.log.latestIdx())
			}

			if len(args.Entries) > 0 {
				rf.dPrintf("rf.AppendEntries", "%v entries are appended, %v entries in log", len(args.Entries), rf.log.size())
			}
		}
	}

	rf.mu.Unlock() //careful
	rf.applyCommands()
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
			rf.resetTerm(reply.Term)
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
