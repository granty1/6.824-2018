package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	STATE_FOLLOWER  = 0
	STATE_LEADER    = 1
	STATE_CANDIDATE = 2

	SEND_ENTRY_INTERVAL = 50 * time.Millisecond
)

///////tell leader that the commited log can be applied.
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	CommandIndex int // index of log array.
	Command      interface{}
	UseSnapshot  bool   // ignore for lab2; only used in lab3
	Snapshot     []byte // ignore for lab2; only used in lab3
	CommandValid bool
}

type logEntries struct {
	LogIndex int
	Term     int
	Log      interface{}
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mtx       sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state for all state
	currentTerm int
	voteFor     int
	log         []logEntries

	// volatile state: first commit -> then apply
	commitIndex int // init to 0: the index that have been commited.
	lastApplied int // init to 0: the index that have been applied.

	// only for leaders
	nextIndex  []int // init to [leader last applied log index + 1]
	matchIndex []int // init to 0;	for each server, index of highest log entry known to be replicated

	// other fields
	state          int
	beenVotedCount int

	// channels
	chanHeartBeat    chan int
	chanBecomeLeader chan int
	chanCommit       chan int
	chanVoteOther    chan int
	chanApply        chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.
	//rf.mtx.Lock()
	//defer rf.mtx.Unlock()
	return rf.currentTerm, rf.state == STATE_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.log)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type RequestAppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int

	Entries           []logEntries
	LeaderCommitIndex int
	/// prevLogIndex, CommitIndex.
}
type RequestAppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int // used to tell leader that follower's next empty log, or leader can decrease one each time.
}

//
// example RequestVote RPC handler.
//

// rpc request/response should check term to convert self to follower;
// 						should check peer's log info to vote peer.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("[::RequestVote]\n")
	// Your code here.
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	defer rf.persist()

	reply.VoteGranted = false

	// case 1: check term
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { // set term to max. and then maybe become leader.
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
	}
	reply.Term = rf.currentTerm

	// case 2: check log
	isNewer := false
	if args.LastLogTerm == rf.log[len(rf.log)-1].Term {
		isNewer = args.LastLogIndex >= rf.log[len(rf.log)-1].LogIndex
	} else {
		isNewer = args.LastLogTerm > rf.log[len(rf.log)-1].Term
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && isNewer {
		rf.chanVoteOther <- 1
		rf.state = STATE_FOLLOWER
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
	}

}
func (rf *Raft) RequestAppendEntries(args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {
	// Your code here.
	// Q: should candidate append entries?
	//fmt.Println("[::RequestAppendEntries]", args)

	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	defer rf.persist()

	// case 1: check term
	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1
		return
	}

	rf.chanHeartBeat <- 1
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = STATE_FOLLOWER
		rf.voteFor = -1
	}
	reply.Term = args.Term

	// case 2: check log number
	if args.PrevLogIndex > rf.log[len(rf.log)-1].LogIndex {
		reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1
		return
	}

	// case 3: check log term. decrease one each time...
	if args.PrevLogIndex > 0 {
		term := rf.log[args.PrevLogIndex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.log[i].Term != term {
					reply.NextIndex = i + 1
					break
				}
			}
			return
		}
	}

	// step4: success: copy the log.
	if args.PrevLogIndex < 0 {

	} else {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
		reply.NextIndex = rf.log[len(rf.log)-1].LogIndex + 1
	}

	if args.LeaderCommitIndex > rf.commitIndex {
		last := rf.log[len(rf.log)-1].LogIndex
		if args.LeaderCommitIndex > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommitIndex
		}
		rf.chanCommit <- 1
	}
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if ok {
		if rf.state != STATE_CANDIDATE {
			return ok
		}
		if args.Term != rf.currentTerm { // consider the current term's reply
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
		}
		if reply.VoteGranted {
			rf.beenVotedCount++
			if rf.state == STATE_CANDIDATE && rf.beenVotedCount > len(rf.peers)/2 {
				rf.state = STATE_FOLLOWER // ...
				rf.chanBecomeLeader <- 1
			}
		}
	}

	return ok
}
func (rf *Raft) sendAppendEntries(server int, args RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
	//fmt.Printf("[sendAppendEntries][who=%v][term=%v]\n", rf.me, args.Term)

	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)

	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	if ok {
		if rf.state != STATE_LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = STATE_FOLLOWER
			rf.voteFor = -1
			rf.persist()
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0 {
				rf.nextIndex[server] = args.Entries[len(args.Entries)-1].LogIndex + 1
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
		} else {
			rf.nextIndex[server] = reply.NextIndex
		}
	}
	return ok
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mtx.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	args.LastLogIndex = rf.log[len(rf.log)-1].LogIndex
	rf.mtx.Unlock()

	//fmt.Printf("[broadcastRequestVote][Candidate = %v]\n", rf.me)

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}

}

func (rf *Raft) broadcastAppendEntries() {
	//fmt.Printf("[::broadcastAppendEntries][Candidate = %v]", rf.me)
	rf.mtx.Lock()
	defer rf.mtx.Unlock()

	N := rf.commitIndex

	last := rf.log[len(rf.log)-1].LogIndex

	// step1: iterate all commitLog
	for i := rf.commitIndex + 1; i <= last; i++ {
		num := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= i && rf.log[i].Term == rf.currentTerm {
				num++
			}
		}
		// replicated in majority of node.
		if 2*num > len(rf.peers) {
			N = i
		}
	}

	// step2: we can apply these logs.
	if N != rf.commitIndex {
		rf.commitIndex = N
		rf.chanCommit <- 1 /// majority of nodes have commited, then we can move applyIndex //
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == STATE_LEADER {
			if rf.nextIndex[i] > 0 {
				// step3: nextIndex[node i] until the end.
				var args RequestAppendEntriesArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = make([]logEntries, len(rf.log[args.PrevLogIndex+1:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1:])
				args.LeaderCommitIndex = rf.commitIndex
				go func(i int, args RequestAppendEntriesArgs) {
					var reply RequestAppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i, args)
			}
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mtx.Lock()
	defer rf.mtx.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	if isLeader {
		index = rf.log[len(rf.log)-1].LogIndex + 1
		rf.log = append(rf.log, logEntries{Term: term, Log: command, LogIndex: index}) // append new entry from client
		rf.persist()
	}
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

func (rf *Raft) working() {
	for {
		switch rf.state {
		case STATE_LEADER:
			rf.broadcastAppendEntries()
			time.Sleep(SEND_ENTRY_INTERVAL)
		case STATE_FOLLOWER:
			select {
			case <-rf.chanHeartBeat: // RequestAppendEntries
			case <-rf.chanVoteOther: // RequestVote
			case <-time.After(time.Duration(rand.Int63()%333+500) * time.Millisecond):
				rf.state = STATE_CANDIDATE
			}
		case STATE_CANDIDATE:
			rf.mtx.Lock()
			rf.currentTerm++
			rf.voteFor = rf.me // voteFor changed with currentTerm.
			rf.beenVotedCount = 1
			rf.persist()
			rf.mtx.Unlock()

			go rf.broadcastRequestVote()

			select {
			case <-time.After(time.Duration(rand.Int63()%333+500) * time.Millisecond):
			case <-rf.chanHeartBeat:
				rf.state = STATE_FOLLOWER
			case <-rf.chanBecomeLeader:
				rf.mtx.Lock()
				rf.state = STATE_LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := range rf.peers {
					rf.nextIndex[i] = rf.log[len(rf.log)-1].LogIndex + 1 // initialize.
					rf.matchIndex[i] = 0
				}
				rf.mtx.Unlock()
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

	// Your initialization code here.
	rf.state = STATE_FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = append(rf.log, logEntries{Term: 0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.chanBecomeLeader = make(chan int, 100)
	rf.chanCommit = make(chan int, 100)
	rf.chanHeartBeat = make(chan int, 100)
	rf.chanVoteOther = make(chan int, 100)
	rf.chanApply = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.working()

	go func() {
		for {
			select {
			case <-rf.chanCommit:
				rf.mtx.Lock()
				commitIndex := rf.commitIndex
				for i := rf.lastApplied + 1; i <= commitIndex; i++ {
					msg := ApplyMsg{CommandIndex: i, Command: rf.log[i].Log, CommandValid: true}
					applyCh <- msg
					rf.lastApplied = i
				}
				rf.mtx.Unlock()
			}
		}
	}()

	return rf
}
