package raft

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DebugCM = 1

type CMState int

const (
	Follower CMState = iota
	Candidate
	Leader
	Dead
)

func (cms CMState) String() string {
	switch cms {
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

type Consensus struct {
	mu      *sync.Mutex
	id      int
	peerIDs []int
	server  *Server

	// 所有服务器都需要持久化存储的 Raft state
	currentTerm int

	state              CMState
	electionResetEvent time.Time
}

func (c *Consensus) runElectionTimer() {
	timeoutDuration := c.electionTimeout()
	c.mu.Lock()
	termStarted := c.currentTerm
	c.mu.Unlock()

	c.dlogf("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// 循环会在以下条件结束：
	//   1 - 发现不再需要选举定时器
	//   2 - 选举定时器超时，CM变为候选人
	//   对于追随者而言，定时器通常会在CM的整个生命周期中一直在后台运行。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		c.mu.Lock()

		// 不再需要计时器
		if c.state != Follower && c.state != Candidate {
			c.dlogf("in election timer state=%s, bailing out", c.state)
			c.mu.Unlock()
			return
		}

		// 任期变化
		if termStarted != c.currentTerm {
			c.dlogf("election timer term changed from %d to %d, exit", termStarted, c.currentTerm)
			c.mu.Unlock()
			return
		}

		// 如果在超时之前没有收到领导者的信息或者给其它候选人投票，就开始新一轮选举
		if elapsed := time.Since(c.electionResetEvent); elapsed >= timeoutDuration {
			c.startElection()
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}

}

func (c *Consensus) electionTimeout() time.Duration {
	// 如果设置了 RAFT_FORCE_MORE_REELECTION, 会有意地经常返回硬编码的数字来进行压力测试。
	// 这会造成不同服务器之间的冲突，迫使集群出现更多的重新选举
	if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
		return time.Duration(150) * time.Millisecond
	}
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (c *Consensus) dlogf(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] %s", c.id, format)
		log.Printf(format, args...)
	}
}

func (c *Consensus) startElection() {

}

func (c *Consensus) becomeFollower() {

}

func (c *Consensus) startLeader() {

}

func (c *Consensus) leaderSendHeartbeats() {

}
