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
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

import "../labgob"

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
	IsSnapshot   bool
	Snapshot     []byte
}

type Log_entry struct {
	Term              int
	UnsimplifiedIndex int
	Command           interface{}
}

const (
	follower = iota
	candidate
	leader
)

const (
	Selection_timeout  = time.Millisecond * 350
	Heartbreak_timeout = time.Millisecond * 100
)

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
	state              int
	currentTerm        int
	votedFor           int
	log_record         []Log_entry
	commitIndex        int
	lastApplied        int
	lastIncludedIndex  int
	lastIncludedTerm   int
	nextIndex          []int
	matchIndex         []int
	applyCh            chan ApplyMsg
	cond               *sync.Cond
	selecetion_timeout time.Duration
	last_accessed_time time.Time
}

func (rf *Raft) SetLastTerm(term int) {
	rf.lastIncludedTerm = term
}

func (rf *Raft) GetLastIndex() int {
	return rf.lastIncludedIndex
}

func (rf *Raft) GetLog() []Log_entry {
	return rf.log_record
}

func (rf *Raft) GetLastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) GetPersister() *Persister {
	return rf.persister
}

func Max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("启动快照\n")
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.last_accessed_time = time.Now()
	}

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedTerm == rf.lastIncludedTerm && args.LastIncludedIndex == rf.lastIncludedIndex {
		return
	}

	if args.LastIncludedIndex <= rf.toUnsimplifiedIndex(len(rf.log_record)-1) {
		if args.LastIncludedTerm == rf.log_record[rf.toSimplifiedIndex(args.LastIncludedIndex)].Term {
			rf.log_record = append([]Log_entry{{Term: args.LastIncludedTerm}}, rf.log_record[rf.toSimplifiedIndex(args.LastIncludedIndex)+1:]...)
		} else {
			rf.log_record = []Log_entry{{Term: args.LastIncludedTerm}}
		}
	} else {
		rf.log_record = []Log_entry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.persistAndSaveSnapshot(args.Data)

	msg := ApplyMsg{
		CommandValid: false,
		IsSnapshot:   true,
		Snapshot:     rf.persister.ReadSnapshot(),
	}

	rf.applyCh <- msg
}

func (rf *Raft) ApplyLog(applyCh chan ApplyMsg) {
	for {
		time.Sleep(time.Millisecond * 15)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			Am := ApplyMsg{}
			Am.Command = rf.log_record[rf.toSimplifiedIndex(rf.lastApplied)].Command
			Am.CommandValid = true
			Am.CommandIndex = rf.lastApplied
			applyCh <- Am
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == leader {
		isleader = true
	} else {
		isleader = false
	}
	// Your code here (2A).
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) saveRfstate() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log_record)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	data := rf.saveRfstate()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistAndSaveSnapshot(snapshot []byte) {
	data := rf.saveRfstate()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log_entry
	var lastIncludedTerm int
	var lastIncludedIndex int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&lastIncludedIndex) != nil {
		panic("readPersist decode fail")
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log_record = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	Client    int64
	Seq       int64
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []Log_entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) becomeCandidate() {
	rf.state = candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.last_accessed_time = time.Now()
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.toUnsimplifiedIndex(len(rf.log_record))
		rf.matchIndex[i] = 0
	}
	go rf.actAsLeader()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.last_accessed_time = time.Now()

	if rf.state == candidate {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
		reply.Success = true
	} else if rf.state == leader {
		reply.Term = rf.currentTerm
		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
			reply.Success = true
		} else {
			reply.Success = false
			return
		}
	} else if rf.state == follower {
		reply.Term = rf.currentTerm

		if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		}

		if rf.toSimplifiedIndex(args.PrevLogIndex) > len(rf.log_record)-1 {
			reply.Success = false
			reply.ConflictIndex = rf.toUnsimplifiedIndex(len(rf.log_record))
			reply.ConflictTerm = -1
			return
		}
		fmt.Printf("prevlog: %d\n", args.PrevLogIndex)
		fmt.Printf("rf.lastInculded: %d\n", rf.lastIncludedIndex)
		fmt.Printf("sim: %d\n", rf.toSimplifiedIndex(args.PrevLogIndex))
		if rf.toSimplifiedIndex(args.PrevLogIndex) < 0 {
			args.PrevLogIndex++
		}
		if args.PervLogTerm != rf.log_record[rf.toSimplifiedIndex(args.PrevLogIndex)].Term {
			reply.ConflictTerm = rf.log_record[rf.toSimplifiedIndex(args.PrevLogIndex)].Term
			index := args.PrevLogIndex - 1
			for rf.toSimplifiedIndex(index) > 0 && rf.log_record[rf.toSimplifiedIndex(index)].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
			reply.Success = false
			return
		}

		if len(args.Entries) > 0 {
			for i, entry := range args.Entries {
				index := args.PrevLogIndex + i + 1
				if rf.toSimplifiedIndex(index) > len(rf.log_record)-1 || rf.log_record[rf.toSimplifiedIndex(index)].Term != entry.Term {
					rf.log_record = rf.log_record[:rf.toSimplifiedIndex(index)]
					rf.log_record = append(rf.log_record, append([]Log_entry{}, args.Entries[i:]...)...)
					rf.persist()
					break
				}
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if rf.toSimplifiedIndex(args.LeaderCommit) <= len(rf.log_record)-1 {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.toUnsimplifiedIndex(len(rf.log_record) - 1)
			}
		}
		reply.Success = true
	}
}

func (rf *Raft) actAsLeader() {
	for {
		rf.mu.Lock()
		if rf.state != leader {
			fmt.Printf("server%d quit leader at term%d\n", rf.me, rf.currentTerm)
			rf.mu.Unlock()
			return
		}
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.lastIncludedIndex >= rf.nextIndex[i] {
				rf.applySnapshotTo(i)
				continue
			}
			args := AppendEntriesArgs{}
			args.LeaderId = rf.me
			args.Term = rf.currentTerm
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PervLogTerm = rf.log_record[rf.toSimplifiedIndex(args.PrevLogIndex)].Term
			args.Entries = []Log_entry{}
			for j := rf.nextIndex[i]; j <= rf.toUnsimplifiedIndex(len(rf.log_record)-1); j++ {
				args.Entries = append(args.Entries, rf.log_record[rf.toSimplifiedIndex(j)])
			}
			reply := AppendEntriesReply{}
			go func(i int, args AppendEntriesArgs, reply AppendEntriesReply) {
				_, isLeader := rf.GetState()
				rf.mu.Lock()
				if isLeader == false || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					_, isLeader := rf.GetState()
					if isLeader == false || rf.currentTerm != args.Term {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > args.Term {
						rf.becomeFollower(reply.Term)
					}
					if reply.Term <= args.Term && reply.Success == false {
						index := -1
						found := false
						for i, entry := range rf.log_record {
							if entry.Term == reply.ConflictTerm {
								index = i
								found = true
							} else if found {
								break
							}
						}
						if found {
							rf.nextIndex[i] = rf.toUnsimplifiedIndex(index + 1)
						} else {
							rf.nextIndex[i] = reply.ConflictIndex
						}
					} else if reply.Success == true {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
					}
					for i := rf.commitIndex + 1; i <= rf.toUnsimplifiedIndex(len(rf.log_record)-1); i++ {
						count := 0
						for j := range rf.peers {
							if rf.matchIndex[j] >= i {
								count++
							}
						}
						if count >= len(rf.peers)/2 && rf.log_record[rf.toSimplifiedIndex(i)].Term == rf.currentTerm {
							rf.commitIndex = i
						}
					}
				}

			}(i, args, reply)
		}
		rf.mu.Unlock()
		time.Sleep(Heartbreak_timeout)
	}
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) Judge_update(args *RequestVoteArgs) bool {
	if len(rf.log_record) == 1 {
		return true
	}
	if rf.log_record[len(rf.log_record)-1].Term < args.LastLogTerm {
		return true
	} else if rf.log_record[len(rf.log_record)-1].Term == args.LastLogTerm {
		if rf.toUnsimplifiedIndex(len(rf.log_record)-1) <= args.LastLogIndex {
			return true
		}
	}
	return false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		fmt.Printf("server%d at term%d refuse vote to server%d at term%d because of term\n", rf.me, rf.currentTerm, args.CandidateID, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.becomeFollower(args.Term)
	}
	if !rf.Judge_update(args) || !(rf.votedFor == -1 || rf.votedFor == args.CandidateID) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.last_accessed_time = time.Now()
	rf.votedFor = args.CandidateID
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
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
func (rf *Raft) ticker() {
	for {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader {
			rf.mu.Unlock()
			continue
		}
		if time.Now().Sub(rf.last_accessed_time) >= rf.selecetion_timeout {
			rf.becomeCandidate()
			total_vote := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				} else {
					args := RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateID = rf.me
					args.LastLogIndex = rf.toUnsimplifiedIndex(len(rf.log_record) - 1)
					args.LastLogTerm = rf.log_record[rf.toSimplifiedIndex(args.LastLogIndex)].Term
					reply := RequestVoteReply{}
					go func(i int, args RequestVoteArgs, reply RequestVoteReply) {
						rf.mu.Lock()
						if rf.state != candidate || rf.currentTerm != args.Term {
							rf.mu.Unlock()
							return
						}
						rf.mu.Unlock()
						ok := rf.sendRequestVote(i, &args, &reply)
						if ok {
							rf.mu.Lock()
							if rf.state != candidate || rf.currentTerm != args.Term {
								rf.mu.Unlock()
								return
							}
							if reply.Term > rf.currentTerm {
								rf.becomeFollower(reply.Term)
								total_vote = -1000
								rf.mu.Unlock()
								return
							}
							if reply.VoteGranted == true && rf.state == candidate {
								fmt.Printf("server%d at term%d vote server%d at term%d\n", i, reply.Term, rf.me, rf.currentTerm)
								total_vote++
							}
							if total_vote > len(rf.peers)/2 && rf.state != leader {
								fmt.Printf("server%d becomes leader at term%d and has %d votes\n", rf.me, rf.currentTerm, total_vote)
								total_vote = -1000
								go rf.becomeLeader()
							}
							rf.mu.Unlock()
						}
					}(i, args, reply)
				}
			}
		}
		rf.mu.Unlock()
	}
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
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B).
	if rf.state != leader {
		return index, term, false
	}

	logEntry := Log_entry{Term: rf.currentTerm, Command: command}
	rf.log_record = append(rf.log_record, logEntry)
	rf.persist()
	term = rf.currentTerm
	index = rf.toUnsimplifiedIndex(len(rf.log_record) - 1)
	fmt.Printf("leader server%d append entry %d\n", rf.me, command)
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

func (rf *Raft) toUnsimplifiedIndex(simplifiedIndex int) int {
	return simplifiedIndex + rf.lastIncludedIndex
}

func (rf *Raft) toSimplifiedIndex(unsimplified int) int {
	return unsimplified - rf.lastIncludedIndex
}

func (rf *Raft) DeleteLog(unsimplifiedIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if unsimplifiedIndex <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedTerm = rf.log_record[rf.toSimplifiedIndex(unsimplifiedIndex)].Term
	rf.log_record = append([]Log_entry{{Term: rf.lastIncludedTerm}}, rf.log_record[rf.toSimplifiedIndex(unsimplifiedIndex)+1:]...)
	rf.lastIncludedIndex = unsimplifiedIndex
	rf.persistAndSaveSnapshot(snapshot)
}

func (rf *Raft) applySnapshotTo(i int) {
	DPrintf("[%v] applySnapshotTo %v", rf.me, i)
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	go func() {
		ok := rf.sendInstallSnapshot(i, &args, &reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			return
		}

		rf.nextIndex[i] = args.LastIncludedIndex + 1
		rf.matchIndex[i] = args.LastIncludedIndex
	}()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	rf.applyCh = applyCh
	rf.votedFor = -1
	rf.cond = sync.NewCond(&sync.Mutex{})
	rf.currentTerm = 0
	rf.state = follower
	rf.selecetion_timeout = Selection_timeout + time.Duration(rand.Intn(100))*time.Millisecond
	rf.last_accessed_time = time.Now()
	rf.log_record = make([]Log_entry, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	go rf.ticker()
	go rf.ApplyLog(applyCh)
	return rf
}
