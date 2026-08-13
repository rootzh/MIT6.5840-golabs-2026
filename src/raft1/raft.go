package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type ServerState int

const (
	Follower ServerState = iota + 1
	Candidate
	Leader
)

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// Persistent state on all servers
	currentTerm uint64
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex     uint64
	lastApplied     uint64
	leader          int
	state           ServerState
	lastRecivedTime time.Time
	electionTimeout time.Duration

	// Volatile state on leaders
	nextIndex           []uint64
	matchIndex          []uint64
	lastSendAppendEntry []time.Time
}

func (rf *Raft) becomeLeaderWithLocked() {
	if rf.state == Leader || rf.state != Candidate {
		return
	}
	tester.Annotate(fmt.Sprintf("Server %d", rf.me), "become leader", fmt.Sprintf("term: %v", rf.currentTerm))
	lastLog := rf.log[len(rf.log)-1]
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		rf.nextIndex[server] = lastLog.Index + 1
		rf.matchIndex[server] = 0
		rf.lastSendAppendEntry[server] = time.Now().Add(-24 * time.Hour)
	}
	rf.state = Leader
	rf.sendHeatbeatWithLocked()
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "become leader end", fmt.Sprintf("term: %v", rf.currentTerm))
}

func (rf *Raft) checkAndMaySetTermAndStateWithLocked(term uint64) {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.votedFor = -1
		if rf.state != Follower {
			rf.state = Follower
		}
		rf.persist()
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.currentTerm)
	isleader = rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term uint64
	var voteFor int
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		tester.Annotate(fmt.Sprintf("Server %d", rf.me), "decode data fail", fmt.Sprintf("data:%v, currentTerm:%d log:%d, votefor:%d", data, rf.currentTerm, len(rf.log), rf.votedFor))
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         uint64
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        uint64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		tester.Annotate(fmt.Sprintf("Server %d", rf.me), "vote currentTerm > args.Term", fmt.Sprintf("args:%v, currentTerm:%d log:%d, votefor:%d", args, rf.currentTerm, len(rf.log), rf.votedFor))
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.checkAndMaySetTermAndStateWithLocked(args.Term)
	}

	lastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
		tester.Annotate(fmt.Sprintf("Server %d", rf.me), "log not match", fmt.Sprintf("request:%d, currentTerm:%d, args term:%d, args log:%d-%d, current log:%d-%d, state:%d", args.CandidateId, rf.currentTerm, args.Term, args.LastLogIndex, args.LastLogTerm, lastLog.Index, lastLog.Term, rf.state))
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.lastRecivedTime = time.Now()
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		tester.Annotate(fmt.Sprintf("Server %d", rf.me), "vote args node", fmt.Sprintf("args:%v, currentTerm:%d log:%d, votefor:%d", args, rf.currentTerm, len(rf.log), rf.votedFor))
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.persist()
		return
	}
	tester.Annotate(fmt.Sprintf("Server %d", rf.me), "vote other node", fmt.Sprintf("args:%v, currentTerm:%d log:%d, votefor:%d", args, rf.currentTerm, len(rf.log), rf.votedFor))
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	tester.Annotate(fmt.Sprintf("Server %d", rf.me), "send request vote begin", fmt.Sprintf("to server:%d, args:%+v, reply:%+v", server, args, reply))
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntryArgs struct {
	Term         uint64
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntryReply struct {
	Term    uint64
	Success bool

	XTerm  uint64 // term in the conflicting entry (if any)
	XIndex uint64 //index of first entry with that term (if any)
	XLen   int    // log length
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// start := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.lastRecivedTime = time.Now()
	rf.checkAndMaySetTermAndStateWithLocked(args.Term)
	if rf.state != Follower {
		tester.Annotate(fmt.Sprintf("Server %d", rf.me), "state not current", fmt.Sprintf("term:%d-%d-%d, state:%d", rf.currentTerm, args.Term, reply.Term, rf.state))
		rf.state = Follower
	}

	preLogIndex := args.PrevLogIndex - rf.log[0].Index
	if preLogIndex >= uint64(len(rf.log)) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = 1
		reply.XIndex = 1
		reply.XLen = len(rf.log)
		return
	}
	if rf.log[preLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XTerm = rf.log[preLogIndex].Term
		reply.XIndex = 1
		for index := preLogIndex; index > 0; index-- {
			if rf.log[index].Term != rf.log[preLogIndex].Term {
				reply.XIndex = rf.log[index+1].Index
				break
			}
		}
		reply.XLen = len(rf.log)
		return
	}
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "Append Entry 1", fmt.Sprintf("term:%d-%d-%d, state:%d, cost:%v", rf.currentTerm, args.Term, reply.Term, rf.state, time.Since(start)))
	if len(args.Entries) > 0 {
		// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "add entry before", fmt.Sprintf("log:%+v, args:%+v", rf.log, args))
		curLogIndex := preLogIndex + 1
		argsLogIndex := -1
		needPersist := false
		for index, entry := range args.Entries {
			if curLogIndex >= uint64(len(rf.log)) {
				break
			}
			if rf.log[curLogIndex].Index != entry.Index || rf.log[curLogIndex].Term != entry.Term {
				rf.log = rf.log[0:curLogIndex]
				needPersist = true
				break
			}
			curLogIndex++
			argsLogIndex = index
		}
		if argsLogIndex+1 < len(args.Entries) {
			rf.log = append(rf.log, args.Entries[argsLogIndex+1:]...)
			needPersist = true
			// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "add entry after", fmt.Sprintf("log:%+v, args:%+v", len(rf.log), args))
		}
		if needPersist {
			rf.persist()
		}
		// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "add entry after", fmt.Sprintf("log:%+v, args:%+v", rf.log, args))
	}
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "Append Entry 2", fmt.Sprintf("term:%d-%d-%d, state:%d, cost:%v", rf.currentTerm, args.Term, reply.Term, rf.state, time.Since(start)))
	rf.leader = args.LeaderId
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		rf.applyCond.Signal()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "Append Entry 3", fmt.Sprintf("term:%d-%d-%d, state:%d, cost:%v", rf.currentTerm, args.Term, reply.Term, rf.state, time.Since(start)))
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "send appendentry begin", fmt.Sprintf("to server:%d, args:%+v, reply:%+v", server, args, reply))
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "send appendentry end", fmt.Sprintf("to server:%d, ok:%v, args:%+v, reply:%+v", server, ok, args, reply))
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 0, 0, false
	}
	lastLog := rf.log[len(rf.log)-1]
	rf.log = append(rf.log, LogEntry{
		Index:   lastLog.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	})
	rf.persist()
	tester.Annotate(fmt.Sprintf("Server %d", rf.me), "write entry", fmt.Sprintf("log index:%d, entry:%+v", lastLog.Index+1, command))

	return int(lastLog.Index) + 1, int(rf.currentTerm), rf.state == Leader
}

func calcCost(start time.Time) time.Duration {
	return time.Since(start)
}

func (rf *Raft) startVoteWithLocked() {
	tester.Annotate(fmt.Sprintf("Server %d", rf.me), "start vote", fmt.Sprintf("start vote, term:%d, lastRecivedTime:%v, electionTimeout:%v", rf.currentTerm, time.Since(rf.lastRecivedTime), rf.electionTimeout))
	lastLog := rf.log[len(rf.log)-1]
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	voteGrantedCount := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			defer tester.Annotate(fmt.Sprintf("Server %d", rf.me), "vote cost time", fmt.Sprintf(" term:%d, reqeust server:%v, cost:%v", args.Term, server, calcCost(time.Now())))
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				tester.Annotate(fmt.Sprintf("Server %d", rf.me), "voted fail", fmt.Sprintf("vote reply: %v, voteFrom:%d, term:%d-%d", reply, server, args.Term, reply.Term))
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if args.Term != rf.currentTerm || rf.state != Candidate {
				// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "term or state changed", fmt.Sprintf("vote reply: %v, voteFrom:%d, term:%d-%d-%d, state:%d", reply, server, rf.currentTerm, args.Term, reply.Term, rf.state))
				return
			}
			if reply.VoteGranted {
				tester.Annotate(fmt.Sprintf("Server %d", rf.me), "voted", fmt.Sprintf("vote reply: %v, voteFrom:%d, term:%d-%d-%d", reply, server, rf.currentTerm, args.Term, reply.Term))
				voteGrantedCount++
				if voteGrantedCount > len(rf.peers)/2 {
					rf.becomeLeaderWithLocked()
				}
			} else {
				tester.Annotate(fmt.Sprintf("Server %d", rf.me), "not voted", fmt.Sprintf("vote reply: %v, voteFrom:%d, term:%d-%d-%d", reply, server, rf.currentTerm, args.Term, reply.Term))
				rf.checkAndMaySetTermAndStateWithLocked(reply.Term)
			}
		}(server)
	}
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		{
			rf.mu.Lock()
			if rf.state != Leader && time.Since(rf.lastRecivedTime) > rf.electionTimeout {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = Candidate
				rf.persist()
				rf.startVoteWithLocked()
				rf.lastRecivedTime = time.Now()
			}
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applyLog() {
	logStartIndex := 0
	logEndIndex := 0
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for true {
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		logStartIndex = int(rf.lastApplied)
		logEndIndex = int(rf.commitIndex)
		copyLog := slices.Clone(rf.log[logStartIndex+1 : logEndIndex+1])
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for i := logStartIndex + 1; i <= logEndIndex; i++ {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      copyLog[i-logStartIndex-1].Command,
				CommandIndex: int(i),
			}
			tester.Annotate(fmt.Sprintf("Server %d", rf.me), "commit log", fmt.Sprintf("logindex:%d", i))
		}
		rf.mu.Lock()
	}

}

func (rf *Raft) updateCommitIndexWithLocked(logIndex uint64) {
	for i := int(logIndex - rf.log[0].Index); i >= 0; i-- {
		if rf.log[i].Term != rf.currentTerm {
			continue
		}
		matchCount := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= rf.log[i].Index {
				matchCount++
			}
		}
		if matchCount > len(rf.peers)/2 && rf.log[i].Index > rf.commitIndex {
			rf.commitIndex = rf.log[i].Index
			rf.applyCond.Signal()
			break
		}

	}
}

func (rf *Raft) getLastConflictTermWithLocked(term uint64) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return i
		}
		if rf.log[i].Term < term {
			return -1
		}
	}
	return -1
}

func (rf *Raft) sendHeatbeatWithLocked() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if rf.nextIndex[peer] > rf.log[len(rf.log)-1].Index && time.Since(rf.lastSendAppendEntry[peer]) < 100*time.Millisecond {
			continue
		}
		logBeginIndex := rf.nextIndex[peer] - rf.log[0].Index
		if logBeginIndex-1 >= uint64(len(rf.log)) {
			fmt.Printf("nextIndex:%d, log 0 index:%d, log len:%d", rf.nextIndex[peer], rf.log[0].Index, len(rf.log))
		}
		preLog := rf.log[logBeginIndex-1]
		args := AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: preLog.Index,
			PrevLogTerm:  preLog.Term,
		}
		if rf.nextIndex[peer] <= rf.log[len(rf.log)-1].Index {
			args.Entries = append(args.Entries, rf.log[logBeginIndex:]...)
			rf.nextIndex[peer] = rf.log[len(rf.log)-1].Index + 1
		}
		rf.lastSendAppendEntry[peer] = time.Now()
		go func(server int, args AppendEntryArgs) {
			// start := time.Now()
			reply := AppendEntryReply{}
			ok := rf.sendAppendEntry(server, &args, &reply)
			// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "rpc cost time", fmt.Sprintf("send append entry rpc cost:%v", time.Since(start)))
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if args.Term != rf.currentTerm || rf.state != Leader {
				tester.Annotate(fmt.Sprintf("Server %d", rf.me), "term or state changed", fmt.Sprintf("append entry reply: %v, from:%d, term:%d-%d-%d, state:%d", reply, server, rf.currentTerm, args.Term, reply.Term, rf.state))
				return
			}
			if reply.Success {
				if len(args.Entries) > 0 {
					rf.matchIndex[server] = args.Entries[len(args.Entries)-1].Index
					rf.updateCommitIndexWithLocked(rf.matchIndex[server])
				}
			} else {
				if reply.Term > rf.currentTerm {
					rf.checkAndMaySetTermAndStateWithLocked(reply.Term)
				} else {
					preIndex := args.PrevLogIndex - rf.log[0].Index
					if reply.XLen < int(preIndex) {
						rf.nextIndex[server] = rf.log[0].Index + uint64(reply.XLen)
					} else {
						conflictLogIndex := rf.getLastConflictTermWithLocked(reply.XTerm)
						if conflictLogIndex == -1 {
							rf.nextIndex[server] = reply.XIndex
						} else {
							rf.nextIndex[server] = rf.log[conflictLogIndex].Index + 1
						}
					}
					tester.Annotate(fmt.Sprintf("Server %d", rf.me), "set preLog", fmt.Sprintf("server:%d args:%+v, reply:%+v, nextIndex:%v", server, args, reply, rf.nextIndex[server]))
				}
			}
			// tester.Annotate(fmt.Sprintf("Server %d", rf.me), "send append entry total cost time", fmt.Sprintf("send append entry cost:%v", time.Since(start)))
		}(peer, args)
	}

}

func (rf *Raft) heatbeat() {
	for true {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.sendHeatbeatWithLocked()
		}
		rf.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.leader = -1
	rf.state = Follower
	rf.votedFor = -1
	rf.lastSendAppendEntry = make([]time.Time, len(rf.peers))
	rf.nextIndex = make([]uint64, len(rf.peers))
	rf.matchIndex = make([]uint64, len(rf.peers))
	for server := range peers {
		rf.nextIndex[server] = 1
		rf.matchIndex[server] = 0
	}
	rf.applyCh = applyCh
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) == 0 {
		rf.log = make([]LogEntry, 1)
		rf.log[0] = LogEntry{
			Term:    0,
			Index:   0,
			Command: nil,
		}
		rf.persist()
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heatbeat()
	go rf.applyLog()

	return rf
}
