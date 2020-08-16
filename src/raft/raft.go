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
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

const (
	Follower = iota
	Candidater
	Leader

	AppendEntriesInterval = 100 * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	votedCount  int
	log         []LogEntry
	state       int

	timer *time.Timer

	voteChannel   chan struct{}
	appendChannel chan struct{}

	commitIndex int
	lastApplied int

	// leader
	// 每次选举后重新初始化
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.is(Leader)
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

//func (rf *Raft) latestLog() (int, int) {
//	if len(rf.log) == 0 {
//		return 0, 0
//	}
//	latestLog := rf.log[len(rf.log)-1]
//	return latestLog.Index, latestLog.Term
//}
//
//func (rf *Raft) preLog() (int, int) {
//	if len(rf.log) < 2 {
//		return 0, 0
//	}
//	preLog := rf.log[len(rf.log)-2]
//	return preLog.Index, preLog.Term
//}
//
//func (rf *Raft) logEntries(nextIndex int) []LogEntry {
//	var logs []LogEntry
//	for _, v := range rf.log {
//		if v.Index >= nextIndex {
//			logs = append(logs, v)
//		}
//	}
//	return logs
//}

func (rf *Raft) term() int {
	rf.mu.Lock()
	defer rf.mu.Lock()
	return rf.currentTerm
}

func (rf *Raft) incrementTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	return rf.currentTerm
}

func (rf *Raft) is(state int) bool {
	rf.mu.Lock()
	rf.mu.Unlock()
	return rf.state == state
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
	//LastLogIndex int
	//LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term   int
	Leader int
	//PrevLogIndex int
	//PrevLogTerm  int
	//Entries      []LogEntry
	//LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.change(Follower)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
	// only agree , reset timer
	if reply.VoteGranted {
		go func() {
			rf.voteChannel <- struct{}{}
		}()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = args.Term
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Success = true
		rf.change(Follower)
	} else {
		reply.Success = true
	}
	go func() {
		rf.appendChannel <- struct{}{}
	}()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := false

	//if term, isLeader = rf.GetState(); isLeader {
	//	index, term = rf.latestLog()
	//	index++
	//	rf.log = append(rf.log, LogEntry{
	//		Term:    rf.currentTerm,
	//		Command: command,
	//		Index:   index,
	//	})
	//	rf.commitIndex++
	//}
	//
	//// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) broadcastVoteRequest() {
	fmt.Println("start election")
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.is(Candidater) && rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				rf.mu.Unlock()
				if reply.VoteGranted {
					rf.votedCount++
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.change(Follower)
					}
				}
			} else {
				fmt.Printf("[%d] send vote request fail\n", server)
			}
		}(i)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	args := AppendEntriesArgs{
		Term:   rf.currentTerm,
		Leader: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if rf.is(Leader) && rf.sendAppendEntries(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {

				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.change(Follower)
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) election() {
	rf.incrementTerm()
	rf.votedFor = rf.me
	rf.votedCount++
	rf.timer.Reset(randDuration())
	rf.broadcastVoteRequest()
}

func (rf *Raft) start() {
	rf.timer = time.NewTimer(randDuration())
	for {
		switch rf.state {
		case Follower:
			fmt.Printf("[%d] follower\n", rf.me)
			select {
			case <-rf.voteChannel:
				rf.timer.Reset(randDuration())
			case <-rf.appendChannel:
				rf.timer.Reset(randDuration())
			case <-rf.timer.C:
				rf.mu.Lock()
				rf.change(Candidater)
				rf.mu.Unlock()
			}
		case Candidater:
			fmt.Printf("[%d] candidater\n", rf.me)
			rf.mu.Lock()
			select {
			case <-rf.appendChannel:
				rf.change(Follower)
			case <-rf.timer.C:
				rf.timer.Reset(randDuration())
				rf.election()
			default:
				if rf.votedCount > (len(rf.peers)+1)/2 {
					rf.change(Leader)
				}
			}
			rf.mu.Unlock()
		case Leader:
			fmt.Printf("[%d] leader\n", rf.me)
			rf.broadcastAppendEntries()
			time.Sleep(AppendEntriesInterval)
		}
	}
}

func (rf *Raft) change(state int) {
	if rf.is(state) {
		return
	}
	lastState := rf.state
	switch state {
	case Follower:
		rf.votedFor = -1
	case Candidater:
		rf.election()
	}
	rf.state = state
	tags := []string{"Follower", "Candidater", "Leader"}
	fmt.Printf("[%d] %s change to %s\n", rf.me, tags[lastState], tags[state])
}

func randDuration() time.Duration {
	return time.Duration(rand.Int()%15+15) * 10 * time.Millisecond
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

	rf.state = Follower
	rf.votedFor = -1
	rf.voteChannel = make(chan struct{})
	rf.appendChannel = make(chan struct{})
	// Your initialization code here (2A, 2B, 2C).
	go rf.start()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
