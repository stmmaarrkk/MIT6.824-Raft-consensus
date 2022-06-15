package raft

import (
	"fmt"
)

type VoteRecord struct {
	CandidateId int
	ElecTerm    int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).

	rf.initRVReply(reply)
	if rf.verifyVoteReq(args, reply) {
		rf.logPrintf("vote for %v", args.CandidateId)
		if rf.status != Follower {
			rf.toFollower(nil)
		}

		//Do not update term here, or there will be a infinite election
		rf.setVotedFor(VoteRecord{args.CandidateId, args.Term})
		reply.VoteGranted = true
	}
}

//The election process is not preemptible(guaranteed by lock in last level)
func (rf *Raft) startElection() {
	rf.resetTerm(rf.nextElectionTerm)
	elecTerm := rf.nextElectionTerm
	rf.nextElectionTerm++
	rf.setVotedFor(VoteRecord{rf.me, rf.currentTerm}) //vote for itself
	voteCh := make(chan RequestVoteReply)

	rf.logPrintf("Election for term %v begins", rf.currentTerm)
	rf.sendRequestVoteToAll(elecTerm, voteCh)
	rf.handleVoteResult(elecTerm, voteCh)
}

func (rf *Raft) sendRequestVoteToAll(elecTerm int, voteCh chan RequestVoteReply) {
	for i := 0; i < rf.nPeer; i++ {
		if i != rf.me {
			go func(peerId int, voteCh chan RequestVoteReply) {
				rf.dPrintf("rf.startElection", "send vote to %v", peerId)

				latestLogTerm := -1
				if latestLog, err := rf.log.getLatestEntry(); err == nil {
					latestLogTerm = latestLog.Term
				}

				args := RequestVoteArgs{
					Term:         elecTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.log.latestIdx(),
					LastLogTerm:  latestLogTerm,
				}

				reply := RequestVoteReply{}

				if !rf.sendRequestVote(peerId, &args, &reply) {
					rf.logPrintf("Server %v fail to send vote request to %v\n for election of term %v ", rf.me, peerId, rf.currentTerm)
				}

				voteCh <- reply
			}(i, voteCh)
		}
	}
}

func (rf *Raft) handleVoteResult(elecTerm int, voteCh chan RequestVoteReply) {
	nVote := 1  //including candidate
	nAgree := 1 //including candidate
	for vote := range voteCh {
		if rf.status != Candidate {
			break
		}

		nVote++
		if vote.VoteGranted {
			nAgree++
		}
		rf.nextElectionTerm = Max(rf.nextElectionTerm, vote.Term+1) //update the next election term

		// if elecTerm != rf.currentTerm {
		// 	rf.dPrintf("rf.startElection", "quit election due to unmatched term(cur:%v, elec:%v)", rf.currentTerm, elecTerm)
		// 	break
		// }
		rf.dPrintf("rf.startElection", "vote result: %v, summary: %v/%v/%v(agreed/collected/total)", vote, nAgree, nVote, rf.nPeer)
		if nAgree > rf.nPeer/2 {
			rf.dPrintf("rf.startElection", "Election for term %v end. Summary: %v/%v/%v(agreed/collected/total)", elecTerm, nAgree, nVote, rf.nPeer)
			if rf.toLeader(elecTerm, &rf.mu) {
				go rf.sendAppendEntries()
			}
			break
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) hasVoted(elecTerm int) bool {
	if rf.votedFor.CandidateId == -1 {
		return false
	} else {
		return rf.votedFor.ElecTerm >= elecTerm
	}
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
	if rf.hasVoted(args.Term) {
		reason = "already voted for other candidate"
		result = false
	} else if rf.currentTerm >= args.Term {
		reason = fmt.Sprintf("my term(%v) is greater than that of the candidate(%v)", rf.currentTerm, args.Term)
		result = false
	} else if myLatestLog, err := rf.log.getLatestEntry(); err == nil {
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
