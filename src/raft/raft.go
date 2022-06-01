package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"

	"fmt"
	"time"
)

type PeerState int

const (
	Leader    PeerState = 0 //just prepared, but is not yet in queue
	Candidate PeerState = 1
	Follower  PeerState = 2 //stay in the queue, but is not yet selected
)

const AppendEntriesInterval = 100
const ElectionTimeoutLB = 2 * AppendEntriesInterval
const ElectionTimeoutUB = 3 * AppendEntriesInterval

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/*persistent states*/
	currentTerm int
	votedFor    int
	log         Log //temporary type

	/*volatile states on all servers*/
	state        PeerState
	stateLock    sync.Mutex
	tickerPeriod time.Duration
	nPeer        int
	termLeader   int
	isolated     bool //true by default, will be set to false if receive hearbeat msg

	/*volatile states on leader*/
	snapshotOffset int
	commitIdx      int

	/*for debug*/
	timeoutCount int
}

func (rf *Raft) init() {
	rf.setCurrentTerm(0)
	rf.setVotedFor(-1)
	rf.log = makeLog(nil)

	rf.timeoutCount = 0 //for debug
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader
	return term, isleader
}

//getter and setter for persistent states
func (rf *Raft) setCurrentTerm(currentTerm int) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.currentTerm = currentTerm
	return true //return false if there is an error
}

func (rf *Raft) setVotedFor(votedFor int) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	rf.votedFor = votedFor
	return true //return false if there is an error
}

func (rf *Raft) appendLog(logs []Entry) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	return rf.log.append(logs)
}

func (rf *Raft) setLog(start int, end int, logs []Entry) bool {
	rf.stateLock.Lock()
	defer rf.stateLock.Unlock()
	//remember to save to disk
	return true //return false if there is an error
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	//code for 2A only(delete when proceed to 2B or 2C)

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
}

func (rf *Raft) sendAppendEntries() {
	receiverList := rf.getAllPeers()
	var wg sync.WaitGroup
	successReplies := make(chan AppendEntriesReply, rf.nPeer)
	rf.DPrintf("rf.sendAppendEntries", "is ready for sending AE")
	for r := range receiverList {
		if r != rf.me {
			wg.Add(1)
			go func(wg *sync.WaitGroup, peerId int) {
				defer wg.Done()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.termLeader,
					PrevLogIndex: 1,
					PrevLogTerm:  1,                       //need to be changed
					Entries:      rf.log.getEntries(0, 1), //need to be changed
					LeaderCommit: 0,                       //need to be changed

				}
				reply := AppendEntriesReply{}
				rf.DPrintf("AE", "before sending")
				if !rf.peers[peerId].Call("Raft.AppendEntries", &args, &reply) {
					rf.DPrintf("rf.sendAppendEntries", "fail to send AE to %v due to lost of connection", peerId)
				} else if !reply.Success {
					rf.DPrintf("rf.sendAppendEntries", "AE request denied by server %v", peerId)
				} else {
					successReplies <- reply
					rf.DPrintf("rf.sendAppendEntries", "AE request approved by server %v", peerId)
				}
				rf.DPrintf("AE", "aftersending")
			}(&wg, r)
		}
	}
	wg.Wait()
	rf.DPrintf("rf.sendAppendEntries", "%v of %v AE requests are approved", len(successReplies), rf.nPeer-1)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.DPrintf("rf.AppendEntries", "In term %v received a AE request from %v of term %v", rf.currentTerm, args.LeaderId, args.Term)

	//if the AE is in a valid term, this peer is not consider isolated
	if args.Term >= rf.currentTerm {
		rf.isolated = false
		rf.termLeader = args.LeaderId
	}

	if rf.verifyAEReq(args) {
		if rf.state != Follower {
			rf.DPrintf("rf.AppendEntries", "In term %v change state to follower due to new leader %v", rf.currentTerm, args.LeaderId)
			rf.toFollower(nil)
		}
		reply.Id = rf.me
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term        int
	CandidateId int
	// LastLogIndex int
	// LastLogTerm  int
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
	if rf.validateElection(args) {
		reply.VoteGranted = true
		rf.toFollower(nil)
		rf.setCurrentTerm(args.Term) //important to have this
		rf.setVotedFor(args.CandidateId)
		rf.DPrintf("rf.RequestVote", "vote for %v", args.CandidateId)
	} else {
		reply.VoteGranted = false
		rf.DPrintf("rf.RequestVote", "reject to vote for %v", args.CandidateId)
	}
	reply.Term = rf.currentTerm
}

//The election process is not preemptible(guaranteed by lock in last level)
func (rf *Raft) startElection() {
	rf.currentTerm++    //increase the term
	rf.votedFor = rf.me //vote for itself
	elecTerm := rf.currentTerm
	voteCh := make(chan bool)

	rf.DPrintf("rf.startElection", "Election for term %v begins", rf.currentTerm)

	//assume that all follower replies in time
	for i := 0; i < rf.nPeer; i++ {
		if i != rf.me {
			go func(peerId int, voteCh chan bool) {
				rf.DPrintf("rf.startElection", "send vote to %v", peerId)
				// lastTerm
				args := RequestVoteArgs{
					Term:        elecTerm,
					CandidateId: rf.me,
					// LastLogIndex: lastLogIdx,
					// LastLogTerm: log[]

				}
				reply := RequestVoteReply{}
				if !rf.sendRequestVote(peerId, &args, &reply) {
					fmt.Printf("Server %v fail to send vote request to %v\n for election of term %v ", rf.me, peerId, rf.currentTerm)
				}

				voteCh <- reply.VoteGranted
			}(i, voteCh)
		}
	}

	nVote := 1  //including candidate
	nAgree := 1 //including candidate
	for vote := range voteCh {
		nVote++
		if vote {
			nAgree++
		}

		if elecTerm != rf.currentTerm {
			rf.DPrintf("rf.startElection", "quit election due to unmatched term(cur:%v, elec:%v)", rf.currentTerm, elecTerm)
			break
		}
		rf.DPrintf("rf.startElection", "vote result: %v, summary: %v/%v/%v(agreed/collected/total)", vote, nAgree, nVote, rf.nPeer)
		if nAgree > rf.nPeer/2 {
			rf.DPrintf("rf.startElection", "Election for term %v end. Summary: %v/%v/%v(agreed/collected/total)", elecTerm, nAgree, nVote, rf.nPeer)
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

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := rf.commitIdx
	term := rf.currentTerm
	isLeader := rf.state == Leader
	DPrintf("[rf.Start] peer %v states: commitIdx:%v, term:%v, isLeader:%v", rf.me, index, term, isLeader)
	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		//randomize sleeping time
		time.Sleep(rf.tickerPeriod)
		rf.timeoutCount++
		rf.DPrintf("rf.ticker", "ticker goes off(%v)", rf.timeoutCount)
		switch rf.state {
		case Leader:
			go rf.sendAppendEntries() //may send heartbeat msg or real append entry req
		case Candidate:
			if rf.isolated {
				go rf.startElection()
			} else {
				rf.toFollower(&rf.mu)
			}
		case Follower:
			if rf.isolated {
				rf.toCandidate(&rf.mu)
				go rf.startElection()
			} else {
				rf.isolated = true
			}
		}

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nPeer = len(peers)
	// Your initialization code here (2A, 2B, 2C).
	rf.init()
	// initialize from state persisted before a crash
	rf.toFollower(&rf.mu)
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
