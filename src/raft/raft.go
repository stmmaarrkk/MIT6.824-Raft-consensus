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
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

type PeerStatus int

const (
	Leader    PeerStatus = 0 //just prepared, but is not yet in queue
	Candidate PeerStatus = 1
	Follower  PeerStatus = 2 //stay in the queue, but is not yet selected
)

const AppendEntriesInterval = 100
const NonHeartbeatAEInterval = 1 //one non-heartbeat AppendEntries req will be sent once each NonHeartbeatAEFreq heart beat entries.
const ElectionTimeoutLB = 5 * AppendEntriesInterval
const ElectionTimeoutUB = 10 * AppendEntriesInterval
const CommandBufferCap = 100
const StatePersistPeriod = 100 //period(in ms) of saving state to disk

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

//apply all commands that are committed
func (rf *Raft) applyCommands() {
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.at(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		rf.logPrintf("Command(%v): {%v} has been committed", msg.CommandIndex, msg.Command)
		rf.applyCh <- msg
	}
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
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/*persistent states*/
	currentTerm int
	votedFor    int
	log         *Log //temporary type

	/*volatile states on all servers*/
	status       PeerStatus
	stateLock    sync.Mutex //lock for persistent state
	commitIndex  int
	lastApplied  int
	tickerPeriod time.Duration
	nPeer        int
	// isolated     bool //true by default, will be set to false if receive hearbeat msg

	/*volatile states on leader*/
	nextIndex         []int
	matchIndex        []int
	aERound           int
	matchIndexChanged bool

	/*helper*/
	nextElectionTerm int
	resetTimerCh     chan bool

	/*for debug*/
	timeoutCount int
}

func (rf *Raft) init() {
	//initialize persistent state(do not use setter)
	rf.log = makeLog()
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.timeoutCount = 0                   //for debug
	rf.resetTimerCh = make(chan bool, 15) //must add capacity, or will be blocked
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	rf.log.encode(e)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	rf.dPrintf("rf.persist", "Save currentTerm=%v, votedFor=%v, len(log)%v", rf.currentTerm, rf.votedFor, rf.log.size())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || rf.log.decode(d) != nil {
		log.Panicf("Fail to load raft persistent state")
	}
	rf.nextElectionTerm = rf.currentTerm + 1

	rf.logPrintf("Restore from term %v, votedFor:%v, len(log):%v", rf.currentTerm, rf.votedFor, rf.log.size())
	// rf.dPrintf("rf.readPersist", "restore with log(%v): %v", rf.log.size(), rf.log.entries)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := rf.log.nextIdx()
	term := rf.currentTerm
	isLeader := rf.status == Leader
	// Your code here (2B).
	if isLeader {
		rf.appendLogs(rf.createLog(command))
		rf.logPrintf("Leader %v appended a new command from client into its log. %v entries in the log", rf.me, rf.log.size())
	}
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
// func (rf *Raft) ticker(timerTerm int) {

// 	for !rf.killed() && rf.timerTerm == timerTerm {
// 		// Your code here to check if a leader election should
// 		// be started and to randomize sleeping time using
// 		// time.Sleep().

// 		//randomize sleeping time
// 		time.Sleep(rf.tickerPeriod)
// 		rf.timeoutCount++
// 		rf.dPrintf("rf.ticker", "ticker goes off(%v)", rf.timeoutCount)
// 		switch rf.status {
// 		case Leader:
// 			go rf.sendAppendEntries() //may send heartbeat msg or real append entry req
// 		case Candidate:
// 				go rf.startElection()
// 			} else {
// 				rf.toFollower(&rf.mu)
// 			}
// 		case Follower:
// 			if rf.isolated {
// 				rf.toCandidate(&rf.mu)
// 				go rf.startElection()
// 			} else {
// 				rf.isolated = true
// 			}
// 		}

// 	}
// }

func (rf *Raft) ticker() {
	for !rf.killed() {
		// On each iteration new timer is created
		t := time.NewTimer(rf.tickerPeriod)
		select {
		case <-t.C:
			rf.dPrintf("rf.ticker", "ticker fires(%v)", rf.timeoutCount)
			switch rf.status {
			case Leader:
				go rf.sendAppendEntries() //may send heartbeat msg or real append entry req
			case Candidate:
				go rf.startElection()
			case Follower:
				rf.toCandidate(&rf.mu)
				go rf.startElection()
			}
			rf.timeoutCount++

		case <-rf.resetTimerCh: //Handle income message and move to the next iteration
			rf.dPrintf("rf.ticker", "ticker reset(%v)", rf.timeoutCount)
			if !t.Stop() {
				<-t.C
			}
		}
	}

}

//Implement logic to store the persistent state regularly
func (rf *Raft) statePersistTicker() {
	for !rf.killed() {
		//save states
		time.Sleep(StatePersistPeriod)
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.nPeer = len(peers)
	rf.applyCh = applyCh
	// Your initialization code here (2A, 2B, 2C).
	rf.init()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.toFollower(&rf.mu)

	// start ticker goroutine to start elections
	go rf.ticker()
	// go rf.statePersistTicker() //activate in 2C
	return rf
}
