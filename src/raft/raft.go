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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Role int8

const (
	FOLLOWER Role = iota
	CANDIDATE
	LEADER
)
const HeartBeatTimeout = 100 //心跳超时，要求1秒10次，所以是100ms一次

// LogEntry 日志记录每条日记记录包含状态机的命令
// 和从 leader 接受到日志的任期。
type LogEntry struct {
	Term    int         //哪个任期的log
	Command interface{} //具体命令
}

// Raft A Go object implementing a single Raft peer.
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
	currentTerm   int           //当前任期
	currentLeader int           //当前任期的leader
	votedFor      int           //投票给谁了
	role          Role          //当前身份，记得及时更新
	timer         *time.Timer   //定时器
	timeout       time.Duration //选举超时周期 election timeout
	//lastActiveTime time.Time     //上次活跃时间
	appendCh  chan bool //心跳事件
	voteCh    chan bool //投票事件
	voteCount int       //计票
	log       []*LogEntry

	//所有机器的可变状态
	commitIndex int //将被提交的日志记录的索引(初值为 0 且单调递增) 不需要持久化。
	lastApplied int //已经被提交到状态机的最后一个日志的索引(初值为 0 且单调递增)。

	//作为leader需要管理其他节点的进度
	nextIndex  []int //记录下一条发送到该peer的日志索引
	matchIndex []int //记录将要复制给改peer的日志索引
	ok         bool  //
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == LEADER
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	rf.persister.SaveRaftState(w.Bytes())
}

// restore previously persisted state.
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	Lab2bPrintf("raft %v read persist.log=%v.\n", rf.me, rf.log)
}

// CondInstallSnapshot A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//TODO Lab2D
}

// RequestVoteArgs example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期，候选者用于更新自己的信息
	VoteGranted bool //是否投给它
}

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//收到投票邀请的逻辑
	//1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	//2. 如果本地 voteFor 为空，候选者日志和本地日志相同，则投票给该候选者 (5.2 和 5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	Lab2bPrintf("raft %v(term = %v) get RequestVote from raft %v(term = %v).\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		Lab2aPrintf("raft %v get RequestVote from raft %v,but args.Term < rf.currentTerm\n", rf.me, args.CandidateId)
		return
	}
	if args.Term > rf.currentTerm {
		//对于所有机器：如果 RPC 请求中或者响应中包含任期 T>currentTerm， 参与者 该机器转化为参与者。
		rf.currentTerm = args.Term
		Lab2aPrintf("raft %v currentTerm is %v\n", rf.me, rf.currentTerm)
		rf.switchRoleTo(FOLLOWER)
	}
	//投票逻辑:与候选人最后一条日志的term相同？？

	lastLogTerm := 0 //获取当前节点的最后一条日志的任期
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	//需要保证当前候选者
	vote := (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex+1 >= len(rf.log))
	//log.Print("raft ", rf.me, " : ", args.LastLogTerm, lastLogTerm, args.LastLogIndex+1, len(rf.log))
	Lab2Printf("raft %v get RequestVote from raft %v.args.LastLogTerm=%v,LastLogTerm=%v;args.LastLogIndex+1=%v,len(rf.log)=%v;rf.votedFor=%v.%v\n", rf.me, args.CandidateId, args.LastLogTerm, lastLogTerm, args.LastLogIndex+1, len(rf.log), rf.votedFor, vote)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && vote {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		Lab2Printf("raft %v get RequestVote from raft %v,success!\n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = false
	}
	rf.voteCh <- true
}

// AppendEntriesArgs AppendEntries RPC
type AppendEntriesArgs struct {
	Term              int         //当前leader任期
	LeaderId          int         //用来 follower 重定向到 leader
	PrevLogIndex      int         //前继日志的Index
	PrevLogTerm       int         //前继日志的任期
	Entries           []*LogEntry //存储日志
	LeaderCommitIndex int         //leader的commitIndex
}
type AppendEntriesReply struct {
	Term    int  //当前任期，用于更新leader自己
	Success bool //如果 follower 包含索引为 prevLogIndex 和任期为 prevLogItem 的日志 则为true
}

// AppendEntries leader调用follower，实现日志的复制
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//接受者的实现:
	//1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
	//2. 如果自己不存在索引、任期和 prevLogIndex、prevLogItem匹配的日志返回 false。(5.3)
	//3. 如果存在一条日志索引和 prevLogIndex 相等，但是任期和 prevLogItem 不相同的日志，需要删除这条日志及所有后继日志。(5.3)
	//4. 如果 leader 复制的日志本地没有，则直接追加存储。
	//5. 如果 leaderCommit>commitIndex，设置本地 commitIndex 为 leaderCommit 和最新日志索引中 较小的一个。

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Success = true
	if args.Entries != nil {
		Lab2bPrintf("raft %v(term = %v) get AppendEntries from raft %v(term = %v)\n", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	}
	if args.Term < rf.currentTerm {
		//1. 如果 leader 的任期小于自己的任期返回 false。(5.1)
		//此时这个过期的leader会转换为自己的follower
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	} else if args.Term > rf.currentTerm {
		//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
		//如果碰到了比自己任期大的节点，更新自己的信息
		rf.currentTerm = args.Term
		rf.switchRoleTo(FOLLOWER)
		reply.Term = args.Term
	}

	rf.appendCh <- true //通知，更新定时器
	//if args.Entries == nil {
	//	//心跳
	//	return
	//}

	//核心是找有没有prev
	//特判-1
	if len(rf.log) <= args.PrevLogIndex {
		//2. 如果自己不存在索引、任期和 prevLogIndex、prevLogItem匹配的日志返回 false。(5.3)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.PrevLogIndex != -1 {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			//3. 如果存在一条日志索引和 prevLogIndex 相等，但是任期和 prevLogItem 不相同的日志，需要删除这条日志及所有后继日志。(5.3)
			reply.Success = false
			reply.Term = rf.currentTerm
			rf.log = rf.log[:args.PrevLogIndex]
			return
		}

	}

	//now rf.log[args.PrevLogIndex].Term == args.PrevLogTerm
	//4. 如果 leader 复制的日志本地没有，则直接追加存储。
	//for i, entry := range rf.log {
	//	Lab2bPrintf("raft %v : log[%v]:%v\n", rf.me, i, entry)
	//}
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)

	reply.Success = true
	//5. 如果 leaderCommit>commitIndex，设置本地 commitIndex 为 leaderCommit 和最新日志索引中 较小的一个。
	if args.LeaderCommitIndex > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommitIndex, len(rf.log)-1)
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
// 功能：提交一个command，需要立刻返回，过一段时间这个command会被raft集群提交
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.role == LEADER
	if isLeader == false { //如果不是leader，无效
		return index, term, isLeader
	}
	Lab2bPrintf("raft %v(leader,term:%v) receive command-%v\n", rf.me, rf.currentTerm, command)
	//此时自己是leader，需要将入参存进自己的logs
	index = len(rf.log) + 1
	rf.log = append(rf.log, &LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	})

	//for i, entry := range rf.log {
	//	Lab2bPrintf("raft %v : log[%v]:%v", rf.me, i, entry)
	//}
	rf.persist()
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//核心逻辑：如果选举定时器超时时，没有收到 leader 的追加日志请求或者没有投票给候选者，该机器转化为候选者。
		//1. 每次都需要刷新随机数,选举计时器应该是3~4倍的心跳超时时间
		electionTimeout := HeartBeatTimeout*3 + rand.Intn(HeartBeatTimeout)
		rf.timeout = time.Duration(electionTimeout) * time.Millisecond

		rf.mu.Lock()
		Role := rf.role
		rf.mu.Unlock()

		if Role != CANDIDATE {
			Lab2aPrintf("raft %v(%v,term : %v).\n", rf.me, Role, rf.currentTerm)
		}

		switch Role { //理解为实现一个状态机
		case FOLLOWER:
			{
				//如果此时角色是follower：
				//如果在选举超时之前都没有收到领导人的心跳，或者是候选人的投票请求，就自己变成候选人
				//如何实现这个逻辑？
				//1. 记录上一次活跃时间time.Since。
				//2.channel通讯，如果在计时器到期之前select都没有走向其他分支（其他channel中没有生产者工作，说明没有收到心跳包），则
				//这里用的第二种思路，因为第一种思路需要再很多地方重置计时器，容易出错。借鉴了别人的实现方法
				select {
				case <-rf.voteCh:
					{
						//有投票事件发生，刷新定时器
						Lab2aPrintf("raft %v 有投票事件发生，刷新定时器\n", rf.me)
						rf.timer.Reset(rf.timeout)
					}
				case <-rf.appendCh:
					{
						//有心跳事件发生，刷新定时器
						Lab2aPrintf("raft %v 有心跳事件发生，刷新定时器\n", rf.me)
						rf.timer.Reset(rf.timeout)
					}
				case <-rf.timer.C:
					{
						//定时器到期了，角色转换：此时的逻辑是1.从follower变为Candidate 2.发起选举
						Lab2aPrintf("raft %v 定时器到期，1.从follower变为Candidate 2.发起选举\n", rf.me)
						rf.mu.Lock()
						rf.switchRoleTo(CANDIDATE)
						rf.mu.Unlock()
						rf.startElection()
					}
				}
			}
		case CANDIDATE:
			{
				select {
				case <-rf.appendCh:
					{
						//3. 如果接受到新 leader 的追加日志请求，则转化为参与者。
						rf.timer.Reset(rf.timeout)
						rf.mu.Lock()
						rf.switchRoleTo(FOLLOWER)
						rf.mu.Unlock()
					}
				case <-rf.timer.C:
					{
						//4. 如果选举定时器超时，则重启选举。
						rf.startElection()
					}
				default:
					{
						rf.mu.Lock()
						if rf.voteCount > len(rf.peers)/2 {
							rf.switchRoleTo(LEADER)
						}
						rf.mu.Unlock()
						time.Sleep(70 * time.Millisecond)
					}
				}
			}
		case LEADER:
			{
				go rf.updateLeaderCommitIndex()
				rf.sendAppend()
				time.Sleep(HeartBeatTimeout * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) switchRoleTo(role Role) {

	Lab2aPrintf("raft %v :%v -> %v\n", rf.me, rf.role, role)
	if role == FOLLOWER {
		rf.role = role
		Lab2aPrintf("raft %v become follower in term:%v\n", rf.me, rf.currentTerm)
		rf.votedFor = -1
	}
	if role == CANDIDATE {
		rf.role = role
		Lab2aPrintf("raft %v become candidate in term:%v\n", rf.me, rf.currentTerm)
	}
	if role == LEADER {
		rf.role = role
		rf.ok = false
		Lab2bPrintf("raft %v become leader in term:%v\n", rf.me, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = len(rf.log) - 1
		}
	}
}

func (rf *Raft) startElection() {
	//开始一次选举
	//1. 一旦变为候选者，则开始启动选举:
	//1.1 增加 currentTerm
	//1.2 选举自己
	//1.3 重置选举定时器
	//1.4 并行发送选举请求到其他所有机器
	//2. 如果收到集群大多数机器的选票，则称为新的 leader。
	//3. 如果接受到新 leader 的追加日志请求，则转化为参与者。
	//4. 如果选举定时器超时，则重启选举。

	Lab2aPrintf("raft %v start election.\n", rf.me)
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.timer.Reset(rf.timeout)
	rf.voteCount = 1
	rf.persist()
	rf.mu.Unlock()
	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}
			if len(rf.log) > 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			args.LastLogIndex = len(rf.log) - 1
			rf.mu.Unlock()
			reply := RequestVoteReply{}
			if rf.sendRequestVote(peer, &args, &reply) {
				Lab2aPrintf("Candidate %v requestVote %v,result is %v\n", rf.me, peer, reply.VoteGranted)
				rf.mu.Lock()
				if reply.VoteGranted { //拿到选票
					rf.voteCount += 1
				} else if reply.Term > rf.currentTerm {
					//对于所有机器：如果 RPC 请求中或者响应中包含任期 T>currentTerm， 参与者 该机器转化为参与者。
					rf.currentTerm = reply.Term
					rf.switchRoleTo(FOLLOWER)
				}
				rf.mu.Unlock()
			}
		}(peer)
	}
}

// leader向所有节点复制日志
func (rf *Raft) sendAppend() {
	//LEADER：1. 一旦当选:发送空的追加日志请求(心跳)到其它所有机器;在空闲时间发送心跳，阻止其它机器的选举定时器超时。
	Lab2aPrintf("raft %v send heartBeat!!\n", rf.me)

	for peer, _ := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
		retry:
			if _, isleader := rf.GetState(); !isleader { //如果不是leader了退出这个函数，不再继续发心跳
				return
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      rf.nextIndex[peer] - 1,
				PrevLogTerm:       0,
				Entries:           nil,
				LeaderCommitIndex: rf.commitIndex,
			}
			if args.PrevLogIndex >= 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
			}
			if rf.nextIndex[peer] < len(rf.log) {
				args.Entries = rf.log[rf.nextIndex[peer]:]
			}
			rf.mu.Unlock()

			reply := AppendEntriesReply{}
			Lab2aPrintf("raft %v send heartbeat to raft %v\n", rf.me, peer)

			if rf.sendAppendEntries(peer, &args, &reply) {
				Lab2bPrintf("raft %v(term:%v) send appendEntries %v to raft %v(term:%v).\n", rf.me, rf.currentTerm, args, peer, reply.Term)
				//对于所有机器：如果 RPC 请求中或者响应中包含任期 T>currentTerm， 参与者 该机器转化为参与者。
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					Lab2bPrintf("raft %v -> follower.\n", rf.me)
					rf.currentTerm = reply.Term
					rf.switchRoleTo(FOLLOWER)
					rf.mu.Unlock()
					return
				}
				Lab2bPrintf("raft %v -> raft %v,resp:%v.\n", rf.me, peer, reply.Success)
				if reply.Success {
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.mu.Unlock()
					return
				} else {
					//失败了，需要减少nextIndex[peer]并重试
					//TODO nextIndex的回退逻辑
					rf.nextIndex[peer] /= 2
					rf.mu.Unlock()
					goto retry
				}
			}
		}(peer)
	}

}

func (rf *Raft) updateLeaderCommitIndex() {

	rf.mu.Lock()
	N := rf.commitIndex + 1
	rf.mu.Unlock()
	for {
		if _, isleader := rf.GetState(); !isleader {
			return
		}

		cnt := 1
		//log.Printf("raft %v len(rf.peers):%v\n", rf.me, len(rf.peers))
		//log.Printf("raft %v len(rf.matchIndex):%v\n", rf.me, len(rf.matchIndex))
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				cnt++
			}
		}
		if cnt*2 < len(rf.peers) {
			break
		} else {
			rf.mu.Lock()
			if rf.log[N].Term == rf.currentTerm {
				rf.ok = true
			}
			if rf.ok {
				rf.commitIndex = N
			}
			rf.mu.Unlock()
			N++
		}
	}
	Lab2bPrintf("leader raft %v:commitIndex=%v\n", rf.me, rf.commitIndex)
}

// 如果 commitIndex > lastApplied:增加 lastApplied，并将日志 log[lastApplied] 应用到状态机。
func (rf *Raft) commitWorker() {
	for rf.killed() == false {
		rf.mu.Lock()

		if rf.lastApplied < rf.commitIndex {
			Lab2bPrintf("raft %v(%v,%v),lastAppend:%v,commitAppend:%v\n", rf.me, rf.role, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			// 初始化是-1
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ { //commit日志到与Leader相同
				// listen to messages from Raft indicating newly committed messages.
				// 调用过程才test_test.go -> start1函数中
				// 很重要的是要index要加1 因为计算的过程start返回的下标不是以0开始的
				Lab2bPrintf("taft %v log[%v] append.\n", rf.me, i)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.log[i].Command,
					CommandIndex: i + 1,
					//SnapshotValid: false,
					//Snapshot:      nil,
					//SnapshotTerm:  0,
					//SnapshotIndex: 0,
				}
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
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
	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.currentLeader = -1
	rf.votedFor = -1
	rf.role = FOLLOWER

	//set timer
	electionTimeout := HeartBeatTimeout*3 + rand.Intn(HeartBeatTimeout)
	rf.timeout = time.Duration(electionTimeout) * time.Millisecond
	rf.timer = time.NewTimer(rf.timeout)

	rf.appendCh = make(chan bool, 100)
	rf.voteCh = make(chan bool, 100) //为什么要有容量？同步会导致死锁
	rf.voteCount = 0
	rf.commitIndex = -1
	rf.lastApplied = -1

	//as leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitWorker()
	Lab2Printf("RafeNode : [%d],Started!\n", me)
	return rf
}
