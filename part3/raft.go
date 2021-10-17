package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const DebugCM = 1

// CommitEntry 是Raft向提交通道发送的数据。每一条提交的条目都会通知客户端，
// 表明指令已满足一致性，可以应用到客户端的状态机上。
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

	// storage is used to persist state.
	storage Storage

	// 所有服务器都需要持久化存储的 Raft state
	currentTerm int
	votedFor    int
	log         []LogEntry

	// commitChan is the channel where this CM is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}

	// triggerAEChan is an internal notification channel used to trigger
	// sending new AEs to followers when interesting changes occurred.
	triggerAEChan chan struct{}

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              CState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewConsensusModule creates a new CM with the given ID, list of peer IDs and
// server. The ready channel signals the CM that all peers are connected and
// it's safe to start its state machine.
// NewConsensusModule 方法使用给定的服务器ID、同伴ID列表peerIds以及服务器server来创建一个新的CM实例。
// ready channel用于告知CM所有的同伴都已经连接成功，可以安全启动状态机。
func NewConsensusModule(id int, peerIDs []int, server *Server, storage Storage,
	ready <-chan struct{}, commitChan chan<- CommitEntry) *ConsensusModule {
	cm := new(ConsensusModule)
	cm.id = id
	cm.peerIDs = peerIDs
	cm.server = server
	cm.storage = storage
	cm.state = Follower
	cm.votedFor = -1
	cm.commitIndex = -1
	cm.lastApplied = -1

	cm.commitChan = commitChan
	cm.triggerAEChan = make(chan struct{}, 1)
	cm.newCommitReadyChan = make(chan struct{}, 16)

	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)

	if cm.storage.HasData() {
		cm.restoreFromStorage(cm.storage)
	}

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

	go cm.commitChanSender()
	return cm
}

// Report reports the state of this CM.
func (cm *ConsensusModule) Report() (id int, term int, isLeader bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.id, cm.currentTerm, cm.state == Leader
}

// Submit 方法会向CM呈递一条新的指令。这个函数是非阻塞的;
// 客户端读取构造函数中传入的commit channel，以获得新提交条目的通知。
// 如果当前CM是领导者，并且返回true —— 表示指令被接受了。
// 如果返回false，客户端会寻找新的服务器呈递该指令。
func (cm *ConsensusModule) Submit(command interface{}) bool {
	cm.mu.Lock()
	cm.dlogf("Submit received by %v: %v", cm.state, command)

	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{command, cm.currentTerm})
		cm.persistToStorage()
		cm.dlogf("... log=%v", cm.log)
		cm.mu.Unlock()
		cm.triggerAEChan <- struct{}{}
		return true
	}

	cm.mu.Unlock()
	return false
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
	close(cm.newCommitReadyChan)
}

// restoreFromStorage restores the persistent stat of this CM from storage.
// It should be called during constructor, before any concurrency concerns.
func (cm *ConsensusModule) restoreFromStorage(storage Storage) {
	if termData, ok := cm.storage.Get("currentTerm"); ok {
		d := gob.NewDecoder(bytes.NewBuffer(termData))
		if err := d.Decode(&cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}

	if votedData, ok := cm.storage.Get("votedFor"); ok {
		d := gob.NewDecoder(bytes.NewBuffer(votedData))
		if err := d.Decode(&cm.votedFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("votedFor not found in storage")
	}

	if logData, ok := cm.storage.Get("log"); ok {
		d := gob.NewDecoder(bytes.NewBuffer(logData))
		if err := d.Decode(&cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

// persistToStorage saves all of CM's persistent state in cm.storage.
// Expects cm.mu to be locked.
func (cm *ConsensusModule) persistToStorage() {
	var termData bytes.Buffer
	if err := gob.NewEncoder(&termData).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("currentTerm", termData.Bytes())

	var votedData bytes.Buffer
	if err := gob.NewEncoder(&votedData).Encode(cm.votedFor); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("votedFor", votedData.Bytes())

	var logData bytes.Buffer
	if err := gob.NewEncoder(&logData).Encode(cm.log); err != nil {
		log.Fatal(err)
	}
	cm.storage.Set("log", logData.Bytes())
}

// dlogf records a debug info, if DebugC > 0.
func (cm *ConsensusModule) dlogf(format string, args ...interface{}) {
	if DebugCM > 0 {
		format = fmt.Sprintf("[%d] %s", cm.id, format)
		log.Printf(format, args...)
	}
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
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	cm.dlogf("RequestVote: %+v, [currentTerm=%d, votedFor=%d]", args, cm.currentTerm, cm.votedFor)

	// 请求中的任期大于本地任期，转换为追随者状态
	if args.Term > cm.currentTerm {
		cm.dlogf("... term out of date in RequestVote")
		cm.becomeFollower(args.Term)
	}

	// 任期相同，且未投票或已投票给当前请求伙伴，且候选人的日志满足安全性要求
	// 则返回赞成投票；否则，返回反对投票。
	if cm.currentTerm == args.Term &&
		(cm.votedFor == -1 || cm.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.votedFor = args.CandidateID
		cm.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
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
			cm.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := cm.lastLogIndexAndTerm()
			cm.mu.Unlock()
			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateID:  cm.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
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
//
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

	for _, peerID := range cm.peerIDs {
		cm.nextIndex[peerID] = len(cm.log)
		cm.matchIndex[peerID] = -1
	}
	cm.dlogf("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v",
		cm.currentTerm, cm.nextIndex, cm.matchIndex, cm.log)

	go func(heartbeatTimeout time.Duration) {
		// Immediately send AEs to peers.
		cm.leaderSendAEs()

		t := time.NewTimer(50 * time.Millisecond)
		defer t.Stop()

		// Send periodic heartbeats, as long as still leader.
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer to fire again after heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-cm.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				// todo ???
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				cm.mu.Lock()
				if cm.state != Leader {
					cm.mu.Unlock()
					return
				}
				cm.mu.Unlock()
				cm.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
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

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
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

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
		// vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			// 找到插入点 —— 索引从PrevLogIndex+1开始的本地日志与RPC发送的新条目间出现任期不匹配的位置。
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// 循环结束时：
			//  - logInsertIndex指向本地日志结尾，或者是与领导者发送日志间存在任期冲突的索引位置
			//  - newEntriesIndex指向请求条目的结尾，或者是与本地日志存在任期冲突的索引位置
			if newEntriesIndex < len(args.Entries) {
				cm.dlogf("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				cm.dlogf("... log is now: %v", cm.log)
			}

			// Set commit index.
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = intMin(args.LeaderCommit, len(cm.log)-1)
				cm.dlogf("... setting commitIndex=%d", cm.commitIndex)
				cm.newCommitReadyChan <- struct{}{}
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(cm.log) {
				reply.ConflictIndex = args.PrevLogIndex
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// cm.log[PrevLogIndex].
				reply.ConflictTerm = cm.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if cm.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = cm.currentTerm
	cm.persistToStorage()
	cm.dlogf("AppendEntries reply: %+v", *reply)
	return nil
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts cm's state.
// leaderSendHeartbeats 方法向所有同伴发送心跳，收集各自的回复并调整c的状态值
func (cm *ConsensusModule) leaderSendAEs() {
	cm.mu.Lock()
	savedCurrentTerm := cm.currentTerm
	cm.mu.Unlock()

	for _, peerID := range cm.peerIDs {
		go func(peerID int) {
			cm.mu.Lock()
			nextLogIdx := cm.nextIndex[peerID]
			prevLogIdx := nextLogIdx - 1
			prevLogTerm := -1
			if prevLogIdx >= 0 {
				prevLogTerm = cm.log[prevLogIdx].Term
			}

			entries := cm.log[nextLogIdx:]
			args := AppendEntriesArgs{
				Term:     savedCurrentTerm,
				LeaderID: cm.id,

				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: cm.commitIndex,
				Entries:      entries,
			}
			cm.mu.Unlock()

			cm.dlogf("sending AppendEntries to %v: ni=%d, args=%+v", peerID, nextLogIdx, args)
			var reply AppendEntriesReply

			if err := cm.server.Call(peerID, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mu.Lock()
				defer cm.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					cm.dlogf("term out of date in heartbeat reply")
					cm.becomeFollower(reply.Term)
					return
				}

				if cm.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerID] = nextLogIdx + len(entries)
						cm.matchIndex[peerID] = cm.nextIndex[peerID] - 1

						// 每给一个Follower同步完日志，就会判断是否已经完成半数以上节点同步log
						// 如果半数以上节点完成，commitIndex更新到最新logIndex
						savedCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerID := range cm.peerIDs {
									if cm.matchIndex[peerID] >= i {
										matchCount++
									}
								}

								if matchCount*2 > len(cm.peerIDs)+1 {
									cm.commitIndex = i
								}
							}
						}

						cm.dlogf("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d",
							peerID, cm.nextIndex, cm.matchIndex, cm.commitIndex)
						if cm.commitIndex != savedCommitIndex {
							cm.dlogf("leader sets commitIndex := %d", cm.commitIndex)
							// Commit index changed: the leader considers new entries to be
							// committed. Send new entries on the commit channel to this
							// leader's clients, and notify followers by sending them AEs.
							cm.newCommitReadyChan <- struct{}{}
							cm.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictIndex >= 0 {
							lastIndexOfTerm := -1
							for i := len(cm.log) - 1; i >= 0; i-- {
								if cm.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}

							if lastIndexOfTerm >= 0 {
								cm.nextIndex[peerID] = lastIndexOfTerm + 1
							} else {
								cm.nextIndex[peerID] = reply.ConflictIndex
							}
						} else {
							cm.nextIndex[peerID] = reply.ConflictIndex
						}
						cm.dlogf("AppendEntries reply from %d !success: nextIndex := %d", peerID, nextLogIdx-1)
					}
				}
			}
		}(peerID)
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects cm.mu to be locked.
// lastLogIndexAndTerm 返回最新的日志索引和任期，此方法希望被cm.mu锁住
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastIndex := len(cm.log) - 1
		return lastIndex, cm.log[lastIndex].Term
	}
	return -1, -1
}

// commitChanSender is responsible for sending committed entries on
// cm.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; cm.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
// commitChanSender 负责在cm.commitChan上发送已提交的日志条目。
// 它会监听newCommitReadyChan的通知并检查哪些条目可以发送（给客户端）。
// 该方法应该在单独的后台goroutine中运行；cm.commitChan可能会有缓冲来限制客户端消费已提交指令的速度。
// 当newCommitReadyChan关闭时方法结束。
func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mu.Lock()
		savedTerm := cm.currentTerm
		savedLastApplied := cm.lastApplied
		var entries []LogEntry

		if cm.commitIndex > savedLastApplied {
			entries = cm.log[savedLastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mu.Unlock()
		cm.dlogf("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			cm.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	cm.dlogf("commitChanSender done")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
