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
	"math/rand"
	"sort"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type Role int32

const (
	_ Role = iota
	Leader
	Candidate
	Follower
)

const (
	MIN_INTERVAL       = 250
	MAX_INTERVAL       = 400
	BROADCAST_INTERVAL = 40
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

	// for all servers
	currentTerm int
	votedFor    int
	role        Role
	log         []LogEntry
	commitIndex int               // index of highest log entry known to be committed

	electionTimer *time.Timer

	resetTimerCH chan interface{}

	// for candidate
	votedCnt int

	// for leader
	nextIndex  []int              // for each server, index of the next log entry to send
	matchIndex []int              // for each server, index of the log entry known to be replicated
	applyCh    chan ApplyMsg
}

type LogEntry struct {
	Term int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.role==Leader

	return term, isleader
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

// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term int
	Entries []LogEntry
	LeaderCommit int  // leader's commitIndex
	PrevLogIndex int  // index immediately preceding new ones
	PrevLogTerm  int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term int
	Success bool
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID int
	Term        int
	LastLogIndex int   // index of candidate's last LogEntry
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Granted bool
	Term    int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%v requested vote from %v\n",rf.me,args.CandidateID)
	reply.Granted = false
	reply.Term = rf.currentTerm
	if args.Term<rf.currentTerm  {
		DPrintf("%v reject %v because expired\n",rf.me,args.CandidateID)
		return
	}

	// if candidate's log is more up-to-date
	var upToDate bool
	lastIndex := len(rf.log)-1
	if rf.log[lastIndex].Term < args.LastLogTerm {
		upToDate = true
	} else if rf.log[lastIndex].Term > args.LastLogTerm {
		upToDate = false
	} else {
		upToDate = args.LastLogIndex >= lastIndex
	}

	if !upToDate {
		DPrintf("server %v reject grant candidate %v in term % v because log is not up-to-date\n",rf.me,args.CandidateID,args.Term)
		return
	}

	// if candidate's term is greater, grant
	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
	}

	// if already have voted for another candidate
	if rf.votedFor==args.CandidateID || rf.votedFor == -1 {
		//fmt.Printf("%v grant vote to %v\n",rf.me,args.CandidateID)
		reply.Granted = true
	}

	reply.Term = rf.currentTerm
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// success only if leader is valid and prevEntry matched
	reply.Success = false

	if args.Term<rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTimerCH <- struct{}{}

	if args.Term>rf.currentTerm {
		rf.currentTerm = args.Term
	}

	if rf.role!=Follower {
		rf.setRole(Follower)
	}

	reply.Term = rf.currentTerm

	last:=0  // last entry matched
	prevEntryMatch := args.PrevLogIndex<len(rf.log) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm

	if prevEntryMatch {
		last = args.PrevLogIndex
		reply.Success = true
		if args.Entries!=nil {
			rf.log = rf.log[0:args.PrevLogIndex+1]  // delete conflict entries
			rf.log = append(rf.log, args.Entries[0])
			last+=1
			DPrintf("server %v receive entry term %v, at index: %v\n", rf.me, args.Term, len(rf.log)-1)
		}
	}

	if args.LeaderCommit > rf.commitIndex && prevEntryMatch {
		rf.commitToIndex(min(args.LeaderCommit, last))
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
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Leader {
		index = len(rf.log)
		term = rf.currentTerm
		rf.matchIndex[rf.me] = len(rf.log)
		rf.log = append(rf.log,LogEntry{rf.currentTerm, command})
	} else {
		isLeader = false
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

	// Your initialization code here (2A, 2B, 2C).
	rf.setRole(Follower)
	rf.votedFor = -1
	rf.resetTimerCH = make(chan interface{})
	rf.nextIndex = make([]int, len(rf.peers))
	// log start from 1 for test
	rf.log = make([]LogEntry,1)
	rf.log[0] = LogEntry{
		0,
		nil,
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.resetTimer()
	//fmt.Printf("%v maked\n",rf.me)
	go rf.electionDaemon()

	return rf
}

func (rf *Raft) resetTimer() {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(time.Millisecond*time.Duration(rand.Intn(MAX_INTERVAL - MIN_INTERVAL)+MIN_INTERVAL))
	} else {
		rf.electionTimer.Reset(time.Millisecond*time.Duration(rand.Intn(MAX_INTERVAL - MIN_INTERVAL)+MIN_INTERVAL))
	}
}

func (rf *Raft) setRole(role Role) {
	rf.role = role
}

func (rf *Raft) electionDaemon() {
	//fmt.Printf("%v daemon\n",rf.me)
	for {
		select {
		case <-rf.resetTimerCH:
			rf.resetTimer()
		case <-rf.electionTimer.C:
			rf.resetTimer()
			go rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.votedCnt = 0
	rf.setRole(Candidate)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	lastIndex:=len(rf.log)-1
	args := RequestVoteArgs{rf.me, rf.currentTerm, lastIndex, rf.log[lastIndex].Term}
	//fmt.Printf("%v start elect for term %v\n",rf.me, rf.currentTerm)

	rf.mu.Unlock() // no need lock afterward

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Candidate { // maybe this election already done
					if reply.Granted {
						rf.votedCnt += 1
						if rf.votedCnt == len(rf.peers)/2 { // already vote for itself
							//fmt.Printf("%v became leader for term %v\n",rf.me, rf.currentTerm)
							rf.electionTimer.Stop()                // stop timer until expired
							DPrintf("server %v become leader\n",rf.me)
							rf.setRole(Leader)
							rf.leaderInit()
							go rf.broadcast() // broadcast as leader
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.setRole(Follower)
							rf.resetTimerCH <- struct {}{}
						}
					}
				}
			}
		}(i)
	}
}

// start Append entries rpc
func (rf* Raft) startAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role == Leader {
			if reply.Success {
				// update index state and try to commit
				if args.Entries!=nil{
					rf.matchIndex[server] = rf.nextIndex[server]
					rf.nextIndex[server] += 1 // update next index
					go rf.tryCommit()
					DPrintf("%v match at %v\n",server, rf.matchIndex[server])
				}
			} else {
				if reply.Term > rf.currentTerm {  // Leader expired
					rf.setRole(Follower)
					rf.resetTimerCH <- struct{}{}
					rf.currentTerm = reply.Term
				} else {                          // Try previous entry next time
					rf.nextIndex[server] -= 1
				}
			}
		}
	}
}

// Leader periodically broadcasts
func (rf *Raft) broadcast() {
	for {
		if rf.role!=Leader {
			return
		}
		rf.mu.Lock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			preIndex := rf.nextIndex[i] - 1

			args := AppendEntriesArgs{
				rf.currentTerm,
				nil,
				rf.commitIndex,
				preIndex,
				rf.log[preIndex].Term,
			}

			if rf.nextIndex[i] < len(rf.log) {
				// Todo: append more entries
				args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]])
				//preIndex := rf.nextIndex[i] - 1
				//args.PrevLogIndex = preIndex
				//args.PrevLogTerm = rf.log[preIndex].Term
				DPrintf("leader %v to send in term %v, pre index %v, to server %v\n", rf.me, args.Entries[0].Term,preIndex, i)
			}

			go rf.startAppendEntries(i, args)
		}
		rf.mu.Unlock()

		time.Sleep(BROADCAST_INTERVAL*time.Millisecond)
	}
}

// leader try to commit new entries
func (rf *Raft) tryCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchState := make([]int, len(rf.peers))
	copy(matchState,rf.matchIndex)
	sort.Ints(matchState)

	majority := matchState[len(matchState)/2]  // match index of majority
	rf.commitToIndex(majority)
}

// commit index and all indices preceding index
func (rf *Raft) commitToIndex(index int) {
	if rf.commitIndex<index {
		for i:=rf.commitIndex+1;i<=index&&i<len(rf.log);i++ {
			rf.commitIndex = i
			msg:=ApplyMsg{
				true,
				rf.log[i].Command,
				i,
			}
			rf.applyCh<-msg
			DPrintf("server %v set commit to index %v", rf.me, rf.commitIndex)
		}
	}
}

// initialize leader related states
func (rf *Raft) leaderInit() {
	for i:= range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex[rf.me] = len(rf.log)-1
}

func min(a int, b int) int {
	if a<b {
		return a
	} else {
		return b
	}
}