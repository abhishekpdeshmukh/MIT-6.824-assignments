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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	// "strconv"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logs struct {
	Command string
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//	Persistent state on all servers:

	//	(Updated on stable storage before responding to RPCs)
	currentTerm int    //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int    //candidateId that received vote in current term (or null if none)
	log         []logs //entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:

	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	state string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == "Leader"
	rf.mu.Unlock()
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)

}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int    //leader’s term
	LeaderId     int    //so follower can redirect clients
	PrevLogIndex int    //index of log entry immediately preceding new ones
	PrevLogTerm  int    //term of prevLogIndex entry
	Entries      []logs //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int    //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 {
		if args.Term >= rf.currentTerm {
			reply.Success = true
			rf.mu.Lock()
			//fmt.Printf("My election timeout is reset. I am %v\n", rf.me)
			rf.state = "Follower"
			rf.currentTerm = args.Term
			rf.mu.Unlock()
		} else {
			//fmt.Printf("Navin Election  %v\n", rf.me)
			rf.mu.Lock()
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.mu.Unlock()
		}
	} else {
		fmt.Println("Bekkar haglo **************************************************")
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
	}
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	timerResetCondition := func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.state == "Candidate"
	}
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//fmt.Println(" Election Timeout Begin ")
		rf.mu.Lock()
		if rf.state != "Leader" {
			rf.state = "Candidate"
			// id := rf.me
			rf.mu.Unlock()

			// ms := 150 + rand.Intn(151)
			ms := 50 + (rand.Int63() % 300)
			var t int64
			for {
				// //fmt.Println(" Checking TimerReset Condition ")
				if timerResetCondition() {
					// //fmt.Println(" TimerReset Condition Passed")
					if t <= ms {
						t += 1
						time.Sleep(time.Duration(1) * time.Millisecond)
					} else {
						// fmt.Printf("%v Starting Election ", id)
						rf.StartElection()
						break
					}
				} else {
					//fmt.Printf(" TimeReset Condition Failed for %v\n", id)
					break
				}
			}
		} else if rf.state == "Leader" {
			//fmt.Printf(" I am leader %v\n*********************************************************\n\n", rf.me)
			term := rf.currentTerm
			id := rf.me
			rf.mu.Unlock()
			for server := range rf.peers {
				if server != id {
					//fmt.Println("Inside Append Entries")
					go func(server int, term int) {
						//fmt.Println("Inside Anonymous function")
						args := AppendEntriesArgs{
							Term:    term,
							Entries: make([]logs, 0),
						}

						reply := AppendEntriesReply{}
						for !rf.sendAppendEntries(server, &args, &reply) {

						}
						if !reply.Success {
							// fmt.Println(" Rejected by Followers")
							rf.mu.Lock()
							rf.state = "Follower"
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
						}
					}(server, term)
				}
			}
			// ms := 5 + (rand.Int63() % 20)
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
}

func (rf *Raft) StartElection() {
	//fmt.Println(" starting Election *********!!!!!!!!!!!! Now ")
	rf.mu.Lock()
	votes := 1
	id := rf.me
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	logLen := len(rf.log)
	rf.votedFor = id
	if rf.state == "Follower" {
		return
	}
	rf.mu.Unlock()

	for server := range rf.peers {
		// rf.sendRequestVote(index)
		go func(server int) {

			if server != id {

				reply := RequestVoteReply{}
				req := RequestVoteArgs{
					Term:         currentTerm,
					CandidateId:  id,
					LastLogIndex: currentTerm,
					LastLogTerm:  logLen - 1,
				}
				for !rf.sendRequestVote(id, &req, &reply) {

				}
				rf.mu.Lock()
				if reply.VoteGranted {

					votes += 1
					// //fmt.Println("Added Vote")
					// //fmt.Println("Total Votes " + strconv.Itoa(votes))
					if votes > (len(rf.peers)/2) && rf.state == "Candidate" {
						rf.state = "Leader"
						// fmt.Print("WON THE BLODDY ELECTION !!!!!!!!!!!!!!!!!\n\n\n\n\n*****************************************\n\n\n\n\n")
						rf.mu.Unlock()
						return
					}
				} else {
					rf.state = "Follower"
					rf.currentTerm = reply.Term
				}
				rf.mu.Unlock()

			}
		}(server)

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = "Follower"
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
