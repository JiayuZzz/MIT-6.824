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
	"fmt"
	"labgob"
	"log"
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
	MIN_INTERVAL       = 200
	MAX_INTERVAL       = 500
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
	CommandIndex int           // ==-1 means restore snapshot
	CommandTerm  int
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
	commitIndex int // index of highest log entry known to be committed
	baseIndex   int // index of first entry in log

	electionTimer *time.Timer

	resetTimerCH chan interface{}

	// for candidate
	votedCnt int

	// for leader
	nextIndex  []int // for each server, index of the next log entry to send
	matchIndex []int // for each server, index of the log entry known to be replicated
	applyCh    chan ApplyMsg
}

type LogEntry struct {
	Index   int
	Term    int
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
	isleader = rf.role == Leader

	return term, isleader
}

// store snapshot
func (rf *Raft) Snapshot(lastIndex int, lastTerm int, data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("%v snapshot\n",rf.me)
	if lastIndex < rf.baseIndex {  // expired snapshot
		return
	}
	if lastIndex > rf.lastIndex() {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			lastIndex,
			lastTerm,
			nil,
		}
	} else {
		//fmt.Printf("%d %d\n",rf.indexToOffset(lastIndex),len(rf.log))
		rf.log = rf.log[rf.indexToOffset(lastIndex):]
	}
	rf.baseIndex = lastIndex
	rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), data)
}

func (rf *Raft) getStateBytes() []byte {
	buffer := new(bytes.Buffer)
	e := labgob.NewEncoder(buffer)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return buffer.Bytes()
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
	rf.persister.SaveRaftState(rf.getStateBytes())
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&log) != nil {
		panic("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		if len(rf.log) == 0 {
			//rf.log = append(rf.log, LogEntry{0,nil})
			fmt.Printf("restored a empty log\n")
		}
	}
}

// AppendEntries RPC arguments
type AppendEntriesArgs struct {
	Term         int
	Entries      []LogEntry
	LeaderCommit int // leader's commitIndex
	PrevLogIndex int // index immediately preceding new ones
	PrevLogTerm  int
}

// AppendEntries RPC reply
type AppendEntriesReply struct {
	Term       int
	Success    bool
	FirstIndex int // first index of unmatched term
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	CandidateID  int
	Term         int
	LastLogIndex int // index of candidate's last LogEntry
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

type InstallSnapshotArgs struct {
	Term         int
	LastLogIndex int
	LastLogTerm  int
	Snapshot  []byte
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//fmt.Printf("%v requested vote from %v\n",rf.me,args.CandidateID)
	reply.Granted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("%v reject %v because expired\n", rf.me, args.CandidateID)
		return
	}

	// if candidate's log is more up-to-date
	var upToDate bool
	lastIndex := rf.lastIndex()
	if rf.getEntry(lastIndex).Term < args.LastLogTerm {
		upToDate = true
	} else if rf.getEntry(lastIndex).Term > args.LastLogTerm {
		upToDate = false
	} else {
		upToDate = args.LastLogIndex >= lastIndex
	}

	if !upToDate {
		DPrintf("server %v reject grant candidate %v in term % v because log is not up-to-date\n", rf.me, args.CandidateID, args.Term)
		return
	}

	// if candidate's term is greater, grant
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}

	// if don't have voted for another candidate yet
	if rf.votedFor == -1 {
		//fmt.Printf("%v grant vote to %v\n",rf.me,args.CandidateID)
		rf.resetTimerCH <- struct{}{}
		rf.setRole(Follower)
		reply.Granted = true
		rf.votedFor = args.CandidateID
	}
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// success only if leader is valid and prevEntry matched
	reply.Success = false
	reply.FirstIndex = args.PrevLogIndex + 1 // default

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTimerCH <- struct{}{}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	if rf.role != Follower {
		rf.setRole(Follower)
	}

	reply.Term = rf.currentTerm

	lastMatch := 0 // last entry matched
	prevEntryMatch := args.PrevLogIndex <= rf.lastIndex() && rf.getEntry(args.PrevLogIndex).Term == args.PrevLogTerm

	if prevEntryMatch {
		lastMatch = args.PrevLogIndex
		reply.Success = true
		if args.Entries != nil {
			rf.log = rf.log[0 : rf.indexToOffset(args.PrevLogIndex)+1] // delete conflict entries
			rf.log = append(rf.log, args.Entries...)
			lastMatch += len(args.Entries)
			DPrintf("server %v receive entry term %v, at index: %v\n", rf.me, args.Term, len(rf.log)-1)
		}
	} else {
		// to find nextIndex
		index := args.PrevLogIndex
		if args.PrevLogIndex <= rf.lastIndex() {
			// search the first entry on unmatched term
			term := rf.getEntry(index).Term
			for term == rf.getEntry(index - 1).Term && index > rf.baseIndex+1 {
				index -= 1
			}
		} else {
			index = rf.lastIndex() + 1
		}
		reply.FirstIndex = index
	}

	if args.LeaderCommit > rf.commitIndex && prevEntryMatch {
		//fmt.Printf("%d before commit\n",rf.me)
		rf.commitToIndex(min(args.LeaderCommit, lastMatch))
		//fmt.Printf("%d after commit\n",rf.me)
	}
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.resetTimerCH <- struct{}{}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm

	if args.LastLogIndex > rf.lastIndex() {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			args.LastLogIndex,
			args.LastLogTerm,
			nil,
		}
	} else {
		rf.log = rf.log[rf.indexToOffset(args.LastLogIndex):]
	}
	rf.baseIndex = args.LastLogIndex
	rf.commitIndex = args.LastLogIndex
	rf.persister.SaveStateAndSnapshot(rf.getStateBytes(), args.Snapshot)
	rf.restoreSnapshot()
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

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
	//fmt.Printf("%v start to get lock\n",rf.me)
	rf.mu.Lock()
	//fmt.Printf("%v start got lock\n",rf.me)
	defer rf.mu.Unlock()
	if rf.role == Leader {
		index = rf.lastIndex() + 1
		term = rf.currentTerm
		rf.matchIndex[rf.me] = index
		rf.log = append(rf.log, LogEntry{index, rf.currentTerm, command})
		rf.persist()
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
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{
		0,
		0,
		nil,
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.persister.ReadSnapshot())>0 {
		go rf.restoreSnapshot()
	}
	rf.baseIndex = rf.log[0].Index
	rf.commitIndex = rf.baseIndex
	//fmt.Printf("server %d start, commit index: %d\n",rf.me, rf.commitIndex)
	rf.resetTimer()
	//fmt.Printf("%v maked\n",rf.me)
	go rf.electionDaemon()

	return rf
}

func (rf *Raft) resetTimer() {
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(rand.Intn(MAX_INTERVAL-MIN_INTERVAL)+MIN_INTERVAL))
	} else {
		rf.electionTimer.Reset(time.Millisecond * time.Duration(rand.Intn(MAX_INTERVAL-MIN_INTERVAL)+MIN_INTERVAL))
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
			//fmt.Printf("%d reset timer\n",rf.me)
			rf.resetTimer()
		case <-rf.electionTimer.C:
			//fmt.Printf("%d timeout\n",rf.me)
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
	lastIndex := rf.lastIndex()
	args := RequestVoteArgs{rf.me, rf.currentTerm, lastIndex, rf.getEntry(lastIndex).Term}
	//fmt.Printf("%v start elect for term %v\n",rf.me, rf.currentTerm)
	rf.persist()
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
							rf.electionTimer.Stop() // stop timer until expired
							DPrintf("server %v become leader\n", rf.me)
							rf.setRole(Leader)
							rf.leaderInit()
							go rf.broadcast() // broadcast as leader
						}
					} else {
						if reply.Term > rf.currentTerm {
							rf.setRole(Follower)
							rf.resetTimerCH <- struct{}{}
							rf.currentTerm = reply.Term
							rf.persist()
						}
					}
				}
			}
		}(i)
	}
}

// start Append entries rpc
func (rf *Raft) startAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	if rf.sendAppendEntries(server, args, &reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role == Leader {
			if reply.Success {
				// update index state and try to commit
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] += len(args.Entries) // update next index
				go rf.tryCommit()
				DPrintf("server %v match at %v\n", server, rf.matchIndex[server])
			} else {
				if reply.Term > rf.currentTerm { // Leader expired
					rf.setRole(Follower)
					rf.resetTimerCH <- struct{}{}
					rf.currentTerm = reply.Term
					rf.persist()
				} else { // Try previous entry next time
					rf.nextIndex[server] = reply.FirstIndex
					if rf.nextIndex[server] <= 0 {
						fmt.Printf("next index <0!\n")
					}
				}
			}
		}
	}
}

func (rf *Raft) startInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if rf.sendInstallSnapshot(server, args, &reply) {
		rf.mu.Lock()
		defer  rf.mu.Unlock()
		if rf.role == Leader {
			if reply.Term > rf.currentTerm {  // leader expired
				rf.setRole(Follower)
				rf.resetTimerCH <- struct{}{}
				rf.currentTerm = reply.Term
				rf.persist()
			} else {
				rf.matchIndex[server] = rf.baseIndex
				rf.nextIndex[server] = rf.baseIndex+1
			}
		}
	}
}

// Leader periodically broadcasts
func (rf *Raft) broadcast() {
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		//fmt.Printf("%d broadcast\n",rf.me)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}

			preIndex := min(rf.nextIndex[i]-1, rf.lastIndex())
			if preIndex<rf.baseIndex {  // send snapshot
				data:=rf.persister.ReadSnapshot()
				d := labgob.NewDecoder(bytes.NewBuffer(data))
				var lastIndex, lastTerm int
				if d.Decode(&lastIndex)!=nil || d.Decode(&lastTerm)!=nil {
					log.Panicln("decode snapshot error while send snapshot")
				}
				args := InstallSnapshotArgs{
					rf.currentTerm,
					lastIndex,
					lastTerm,
					data,
				}

				go rf.startInstallSnapshot(i, &args)
			} else {                     // send entries
				preTerm := rf.getEntry(preIndex).Term
				args := AppendEntriesArgs{
					rf.currentTerm,
					nil,
					rf.commitIndex,
					preIndex,
					preTerm,
				}

				if rf.nextIndex[i] <= rf.lastIndex() {
					// append multiple entries
					args.Entries = append(args.Entries, rf.log[rf.indexToOffset(rf.nextIndex[i]):]...)
					//DPrintf("leader %v to send in term %v, pre index %v, to server %v\n", rf.me, args.Entries[0].Term,preIndex, i)
				}

				go rf.startAppendEntries(i, &args)
			}
		}
		rf.mu.Unlock()

		time.Sleep(BROADCAST_INTERVAL * time.Millisecond)
	}
}

func (rf *Raft) restoreSnapshot() {
	msg := ApplyMsg{
		true,
		nil,
		-1,
		-1,
	}

	rf.applyCh <- msg
}

// leader try to commit new entries
func (rf *Raft) tryCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	matchState := make([]int, len(rf.peers))
	copy(matchState, rf.matchIndex)
	sort.Ints(matchState)

	majority := matchState[len(matchState)/2] // match index of majority
	// only commit current term's entry
	if rf.getEntry(majority).Term == rf.currentTerm {
		//fmt.Printf("%d leader: %v, try commit %d\n",rf.me,rf.role==Leader,majority)
		rf.commitToIndex(majority)
		//fmt.Printf("%d done commit\n",rf.me)

	}
}

// commit index and all indices preceding index
func (rf *Raft) commitToIndex(index int) {
	if rf.commitIndex < index {
		for i:=max(rf.commitIndex+1,rf.baseIndex); i <= index && i <= rf.lastIndex(); i++ {
			//fmt.Printf("%d commit %d\n",rf.me, i)
			rf.commitIndex = i
			msg := ApplyMsg{
				true,
				rf.getEntry(i).Command,
				i,
				rf.getEntry(i).Term,
			}
			DPrintf("server %v set commit to index %v", rf.me, rf.commitIndex)
			//fmt.Printf("%v send msg %d to channel\n",rf.me,msg.CommandIndex)
			rf.applyCh <- msg
			//fmt.Printf("%d commit %d done\n",rf.me,i)
			//fmt.Printf("%v send msg to channel done\n",rf.me)
		}
	}
}

// initialize leader related states
func (rf *Raft) leaderInit() {
	for i := range rf.peers {
		rf.matchIndex[i] = rf.baseIndex
		rf.nextIndex[i] = rf.lastIndex() + 1
	}
	rf.matchIndex[rf.me] = rf.lastIndex()
}

func (rf *Raft) lastIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) indexToOffset(index int) int {
	return index - rf.baseIndex
}

func (rf *Raft) getEntry(index int) *LogEntry {
	//fmt.Printf("%d get entry, index:%d, offset:%d, last index:%d\n", rf.me, index, rf.indexToOffset(index), rf.lastIndex())
	return &rf.log[rf.indexToOffset(index)]
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}
