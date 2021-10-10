package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

type CommitEntry struct {
	// 被提交的客户端指令
	Command interface{}

	// 被提交的客户端指令索引
	Index int

	// 指令提交时的任期
	Term int
}

// LogEntry used for record log.
type LogEntry struct {
	Command interface{}
	Term    int
}

type CState int

const (
	Follower CState = iota
	Candidate
	Leader
	Dead
)

func (cs CState) String() string {
	switch cs {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case Dead:
		return "dead"
	default:
		panic("unreachable")
	}
}

type ConsensusModule struct {
	mu sync.Mutex

	// 当前CM服务器的ID
	id int

	// peerIDs 罗列了集群中所有同伴的ID
	peerIDs []int

	// server 包含当前CM的服务器，用于向其它同伴发起RPC请求
	server *Server

	// 所有服务器都需要持久化存储的 Raft state
	currentTerm int
	votedFor    int

	nextIndex  []int
	matchIndex []int

	log                []LogEntry
	commitIndex        int
	newCommitReadyChan chan struct{}

	state              CState
	electionResetEvent time.Time
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.
// NewConsensusModule 方法使用给定的服务器ID、同伴ID列表peerIds以及服务器server来创建一个新的CM实例。
// ready channel用于告知CM所有的同伴都已经连接成功，可以安全启动状态机。
func NewConsensusModule(id int, peerIDs []int, server *Server, ready <-chan struct{}) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIDs = peerIDs
	cm.server = server
	cm.state = Follower
	cm.votedFor = -1

	go func() {
		// The CM is quiescent until ready is signaled; then, it starts a countdown for
		// leader election.
		// 收到ready信号前，CM都是静默的；收到信号之后，就会开始选主倒计时
		<-ready
		cm.mu.Lock()
		cm.electionResetEvent = time.Now()
		cm.mu.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}

type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote rpc
func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.dlogf("RequestVote: %+v, [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlogf("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，且未投票或已投票给当前请求伙伴，则返回赞成投票；否则，返回反对投票。
	if cm.currentTerm == args.Term && (cm.votedFor == -1 || cm.votedFor == args.CandidateID) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.dlogf("... RequestVote reply: %+v", reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout duration.
// electionTimeout 方法生成一个伪随机的选举等待时长
func (cm *ConsensusModule) electionTimeout() time.Duration {
	// 如果设置了 RAFT_FORCE_MORE_REELECTION, 会有意地经常返回硬编码的数字来进行压力测试。
	// 这会造成不同服务器之间的冲突，迫使集群出现更多的重新选举
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// dlogf records a debug info, if DebugC > 0.
func (cm *ConsensusModule) dlogf(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] %s", cm.id, format)
		log.Printf(format, args...)
	}
}

// Report reports the state of this CM.
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Stop stops this CM, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) fot all goroutines to
// exit.
// Stop方法可以暂停当前CM，清除其状态。该方法很快结束，
// 但是所有的goroutine退出可能需要一点时间（取决于 选举等待时间）
func (cm *ConsensusModule) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.state = Dead
	cm.dlogf("becomes dead")
}

// startElection starts a new election with this CM as a candidate.
// Expects cm.mu to be locked.
// startElection 方法会将该CM作为候选人发起新一轮选举，要求cm.mu被锁定
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm++
	savedCurrentTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.votedFor = cm.id

	cm.dlogf("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, cm.log)

	var votesReceived uint32 = 1
	for _, peerID := range cm.peerIDs {
		go func(peerID int) {
			args := RequestVoteArgs{
				Term:        savedCurrentTerm,
				CandidateID: cm.id,
			}
			var reply RequestVoteReply
			cm.dlogf("sending RequestVote to %d: %+v", peerID, args)

			if err := cm.server.Call(peerID, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlogf("received RequestVoteReply: %+v", reply)

				// 状态不是候选人，退出选举（可能退化为追随者，也可能已经胜选成为领导者）
				if cm.state != Candidate {
					cm.dlogf("while waiting for reply, state=%v", cm.state)
					return
				}

				// 存在更高任期（新领导者），转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlogf("term out of date in RequestVoteReply")
					cm.becomeFollower(reply.Term)
					return
				}

				// 候选人选举是最新任期
				if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddUint32(&votesReceived, 1))

						// 获得票数超过一半，选举获胜，成为最新的领导者
						if votes*2 > len(cm.peerIDs)+1 {
							cm.dlogf("wins election with %d votes", votes)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerID)
	}

	// run another election timer, in case this election not successful.
	// 另行启动一个选举定时器，以防本次选举不成功
	go cm.runElectionTimer()
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the CM state changes from follower/candidate or the term changes.

// runElectionTimer实现的是选举定时器。如果我们想在新一轮选举中作为候选人，就要启动这个定时器。
// 该方法是阻塞的，需要在独立的goroutine中运行；它应该用作单次（一次性）选举定时器，
// 因为一旦任期变化或者c状态不是追随者/候选人，该方法就会退出。
func (cm *ConsensusModule) runElectionTimer() {
	timeoutDuration := cm.electionTimeout()
	cm.mu.Lock()
	termStarted := cm.currentTerm
	cm.mu.Unlock()

	cm.dlogf("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// 循环会在以下条件结束：
	//   1 - 发现不再需要选举定时器
	//   2 - 选举定时器超时，c变为候选人
	//   对于追随者而言，定时器通常会在c的整个生命周期中一直在后台运行。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		cm.mu.Lock()

		// 不再需要计时器
		if cm.state != Follower && cm.state != Candidate {
			cm.dlogf("in election timer state=%s, bailing out", cm.state)
			cm.mu.Unlock()
			return
		}

		// 任期变化
		if termStarted != cm.currentTerm {
			cm.dlogf("in election timer term changed from %d to %d, balling out", termStarted, cm.currentTerm)
			cm.mu.Unlock()
			return
		}

		// 如果在超时之前没有收到领导者的信息或者给其它候选人投票，就开始新一轮选举
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
}

// becomeFollower makes CM a follower and resets its state.
// Expects cm.mu to be locked.
// becomeFollower方法将c变为追随者并重置其状态。要求cm.mu锁定
func (cm *ConsensusModule) becomeFollower(term int) {
	cm.dlogf("becomes Follower with term=%d; log=%v", term, cm.log)
	cm.state = Follower
	cm.currentTerm = term
	cm.votedFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}

// startLeader switches CM into a leader state and begins process of heartbeats.
// Expects cm.mu to be locked.
// startLeader方法将c转换为领导者，并启动心跳程序。要求cm.mu锁定
func (cm *ConsensusModule) startLeader() {
	cm.state = Leader
	cm.dlogf("becomes Leader term:%d log:%v", cm.currentTerm, cm.log)

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C
			cm.mu.Lock()
			if cm.state != Leader {
				cm.mu.Unlock()
				return
			}
			cm.mu.Unlock()
		}
	}()
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.state == Dead {
		return nil
	}
	cm.dlogf("AppendEntries: %+v", args)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlogf("... term out of date in AppendEntries")
		cm.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}

	reply.Term = cm.currentTerm
	cm.dlogf("AppendEntries reply: %+v", *reply)
	return nil
}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state
// leaderSendHeartbeats 方法向所有同伴发送心跳，收集各自的回复并调整c的状态值
func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	/*
		for _, peerID := range cm.peerIDs {
			go func(peerID int) {
				cm.mu.Lock()
				nextIdx := cm.nextIndex[peerID]
				prevLogIdx := nextIdx - 1
				prevLogTerm := -1
				if prevLogIdx >= 0 {
					prevLogTerm = cm.log[prevLogIdx].Term
				}

				entries := cm.log[nextIdx:]
				args := AppendEntriesArgs{
					Term:     savedCurrentTerm,
					LeaderID: cm.id,

					PrevLogIndex: prevLogIdx,
					PrevLogTerm:  prevLogTerm,
					LeaderCommit: cm.commitIndex,
					Entries:      entries,
				}
				cm.mu.Unlock()

				cm.dlogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, nextIdx, args)
				var reply AppendEntriesReply

				if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
					cm.mu.Lock()
					defer cm.mu.Unlock()
					if reply.Term > savedCurrentTerm {
						cm.dlogf("term out of date in heatbeat reply")
						cm.becomeFollower(reply.Term)
						return
					}

					if cm.state == Leader && savedCurrentTerm == reply.Term {
						if reply.Success {
							cm.nextIndex[peerID] = nextIdx + len(entries)
							cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1
							cm.dlogf("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerID, cm.nextIndex, cm.matchIndex)

							savedCommitIndex := cm.commitIndex
							for i := cm.commitIndex + 1; i < len(cm.log); i++ {
								if cm.log[i].Term == savedCurrentTerm {
									matchCount := 1
									for _, id := range cm.peerIDs {
										if cm.matchIndex[id] >= i {
											matchCount++
										}
									}

									if matchCount*2 > len(cm.peerIDs)+1 {
										cm.commitIndex = i
									}
								}
							}

							if cm.commitIndex != savedCommitIndex {
								cm.dlogf("leader sets commitIndex := %d", cm.commitIndex)
								cm.newCommitReadyChan <- struct{}{}
							}
						} else {
							cm.nextIndex[peerID] = nextIdx - 1
							cm.dlogf("AppendEntries reply from %d !success: nextIndex := %d", peerID, nextIdx-1)
						}
					}
				}
			}(peerID)
		}
	*/

	for _, peerId := range cm.peerIDs {
		args := AppendEntriesArgs{
			Term:     savedCurrentTerm,
			LeaderID: cm.id,
		}
		go func(peerId int) {
			cm.dlogf("sending AppendEntries to %v: ni=%d, args=%+v", peerId, 0, args)
			var reply AppendEntriesReply
			if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlogf("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}
			}
		}(peerId)
	}
}

func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.dlogf("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{command, cm.currentTerm})
		cm.dlogf("... log=%v", cm.log)
		return true
	}
	return false
}
