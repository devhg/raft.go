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

const DebugC = 1

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
	mu      *sync.Mutex
	id      int
	peerIDs []int
	server  *Server

	// 所有服务器都需要持久化存储的 Raft state
	currentTerm int
	votedFor    int
	log         []LogEntry

	state              CState
	electionResetEvent time.Time
}

// NewConsensusModuleModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.

// NewConsensusModuleModule方法使用给定的服务器ID、同伴ID列表peerIds以及服务器server来创建一个新的CM实例。
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
func (cm *ConsensusModule) RequestVote(req RequestVoteArgs, resp *RequestVoteReply) error {
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
	if DebugC > 0 {
		format = fmt.Sprintf("[%d] %s", cm.id, format)
		log.Printf(format, args...)
	}
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
// startElection方法会将该C作为候选人发起新一轮选举，要求cm.mu被锁定
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
			cm.dlogf("sending vote to %d: %v", peerID, args)

			if err := cm.server.Call(peerID, "ConsensusModuleModule.RequestVote", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				cm.dlogf("receive RequestVoteReply: %v", reply)

				// 状态不是候选人，退出选举（可能退化为追随者，也可能已经胜选成为领导者）
				if cm.state != Candidate {
					cm.dlogf("while waiting for reply, state=%v", cm.state)
					return
				}

				// 存在更高任期（新领导者），转换为追随者
				if reply.Term > savedCurrentTerm {
					cm.dlogf("term out of date in RequestVoteReply")
					cm.becomeFollower()
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
						}
					}
				}
			}
		}(peerID)
	}

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
			cm.dlogf("election timer term changed from %d to %d, exit", termStarted, cm.currentTerm)
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
func (cm *ConsensusModule) becomeFollower() {

}

// startLeader switches CM into a leader state and begins process of heartbeats.
// Expects cm.mu to be locked.
// startLeader方法将c转换为领导者，并启动心跳程序。要求cm.mu锁定
func (cm *ConsensusModule) startLeader() {

}

// leaderSendHeartbeats sends a round of heartbeats to all peers, collects their
// replies and adjusts cm's state
// leaderSendHeartbeats 方法向所有同伴发送心跳，收集各自的回复并调整c的状态值
func (cm *ConsensusModule) leaderSendHeartbeats() {

}
