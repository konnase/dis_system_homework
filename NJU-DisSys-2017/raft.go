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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	//"fmt"
)

// import "bytes"
// import "encoding/gob"

const (
	FOLLOWER = "follower"
	CANDIDATE = "candidate"
	LEADER = "leader"
	SLEEPTIME = 50 * time.Millisecond
)


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}


type LogEntry struct {
	Index int
	Term int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm int   //服务器最后知道的任期号
	votedFor int
	log []LogEntry

	//volatile state on all servers
	commitIndex int  //已知的被提交的最大日志条目的索引值
	lastApplied int  //被状态机执行的最大日志条目的索引值

	nextIndex []int
	matchIndex []int

	//
	state string   //记录当前server的状态
	count int   //记录得票数

	leaderChan chan bool
	grantVoteChan chan bool
	heartBeatChan chan bool



}

func (rf *Raft) getLastLogIndex () int {
	index := len(rf.log) - 1
	return rf.log[index].Index
}

func (rf *Raft) getLastLogTerm () int {
	index := len(rf.log) - 1
	return rf.log[index].Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
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
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogTerm int
	PrevLogIndex int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("requestvote")
	reply.VoteGranted=false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm

	up_to_date := false
	if args.LastLogTerm > rf.getLastLogTerm() {
		up_to_date = true
	}
	if (args.LastLogTerm == rf.getLastLogTerm()) && (args.LastLogIndex >= rf.getLastLogIndex()) {
		up_to_date = true
	}

	//表示候选人的日志比当前server的日志要新
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && up_to_date == true {
		//fmt.Println("比较谁的日志更新")
		rf.grantVoteChan <- true
		reply.Term = args.Term
		reply.VoteGranted =true
		rf.votedFor = args.CandidateId
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != CANDIDATE {
			return ok
		}
		//
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1

		}
		if reply.VoteGranted {
			rf.count+=1
			if rf.state == CANDIDATE && rf.count > len(rf.peers)/2 {
				rf.state = FOLLOWER
				rf.leaderChan <- true

			}
		}
	}

	return ok
}

func (rf *Raft) sendRequestVoteToAllEnd() {
	var arg RequestVoteArgs
	rf.mu.Lock()
	arg.Term = rf.currentTerm
	arg.CandidateId = rf.me
	arg.LastLogIndex = rf.getLastLogIndex()
	arg.LastLogTerm = rf.getLastLogTerm()
	rf.mu.Unlock()
	for peer := range rf.peers {
		if peer != rf.me && rf.state == CANDIDATE {
			go func(peer int) {
				var reply RequestVoteReply
				rf.sendRequestVote(peer, arg, &reply)
			}(peer)

		}
	}
}

func (rf *Raft)AppendEntries (args AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	//Leader发来的AppenEntries的term比当前的还小，则返回false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.heartBeatChan <- true
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft)sendAppendEntries (server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != LEADER {
			return ok
		}
		if args.Term != rf.currentTerm {
			return ok
		}
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return ok
		}
	}
	return ok
}

func (rf *Raft)sendAppendEntriesToAllEnd(){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			go func(i int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(i)
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
	index := -1
	term := -1
	isLeader := true


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

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.grantVoteChan = make(chan bool, 200)
	rf.leaderChan = make(chan bool, 200)
	rf.heartBeatChan = make(chan bool, 200)

	//fmt.Println("make a raft")

	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for  {
			switch rf.state {
			case FOLLOWER:
				select {
				case <- rf.heartBeatChan:
				case <- rf.grantVoteChan:
				case <- time.After(time.Duration(r.Intn(100) + 500) * time.Millisecond):
					rf.state = CANDIDATE
				}
			case LEADER:
				rf.sendAppendEntriesToAllEnd()
				time.Sleep(SLEEPTIME)
			case CANDIDATE:
				rf.mu.Lock()
				rf.currentTerm+=1
				rf.votedFor = me
				rf.count = 1
				rf.mu.Unlock()
				go rf.sendRequestVoteToAllEnd()
					select{
					case <- time.After(time.Duration(r.Intn(130) + 500) * time.Millisecond):
					case <- rf.heartBeatChan:
						rf.state = FOLLOWER
					case <- rf.leaderChan:
						rf.mu.Lock()
						rf.state = LEADER
						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for peer :=range rf.peers {
							rf.nextIndex[peer] = rf.getLastLogIndex() + 1
							rf.matchIndex[peer] = 0
						}
						rf.mu.Unlock()
					}

			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
