package raft

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// Server wraps a raft.ConsensusModule along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for presentation
// purposes. raft.ConsensusModule has a *Server to do its peer communication and
// doesn't have to worry about the specifics of running an
// RPC server.

// Server 包装了raft.ConsensusModule和rpc.Server，并将后者暴露为RPC端点。同时还负责管理Raft服务器的同伴。
// 这样设计的主要目的是简化用于演示的raft.Server代码。
// raft.ConsensusModule可以通过*Server与同伴进行通信，而不必关心RPC服务器的具体细节。
type Server struct {
	mu sync.Mutex

	serverID int
	peerIDs  []int

	cm       *ConsensusModule
	storage  Storage
	rpcProxy *RPCProxy

	rpcServer *rpc.Server
	lis       net.Listener

	commitChan  chan<- CommitEntry
	peerClients map[int]*rpc.Client

	ready <-chan struct{}
	quit  chan interface{}
	wg    sync.WaitGroup
}

func NewServer(serverID int, peerIDs []int, storage Storage, ready <-chan struct{},
	commitChan chan<- CommitEntry) *Server {
	return &Server{
		serverID:    serverID,
		peerIDs:     peerIDs,
		commitChan:  commitChan,
		ready:       ready,
		storage:     storage,
		peerClients: make(map[int]*rpc.Client),
		quit:        make(chan interface{}),
	}
}

func (s *Server) Serve() {
	s.mu.Lock()
	s.cm = NewConsensusModule(s.serverID, s.peerIDs, s, s.storage, s.ready, s.commitChan)

	// Create a new rpc server and register a RPCProxy that forwards all methods to CM.
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{s.cm}
	_ = s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy)

	var err error
	s.lis, err = net.Listen("tcp", ":0")
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[%v] listening at %v", s.serverID, s.lis.Addr())
	s.mu.Unlock()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.lis.Accept()
			if err != nil {
				select {
				case <-s.quit:
					return
				default:
					log.Fatal("accept error: ", err)
				}
			}

			s.wg.Add(1)
			go func() {
				s.rpcServer.ServeConn(conn)
				s.wg.Done()
			}()
		}
	}()
}

// Shutdown closes the server and waits for it to shut down properly.
func (s *Server) Shutdown() {
	s.cm.Stop()
	close(s.quit)
	_ = s.lis.Close()
	s.wg.Wait()
}

// DisconnectAll closes all the client connections to peers for this server.
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, client := range s.peerClients {
		if client != nil {
			err := client.Close()
			if err != nil {
				panic(err)
			}
			delete(s.peerClients, key)
		}
	}
}

func (s *Server) GetListenAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.lis.Addr()
}

// ConnectToPeer .
func (s *Server) ConnectToPeer(peerID int, addr net.Addr) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.peerClients[peerID]; !ok {
		c, err := rpc.Dial(addr.Network(), addr.String())
		if err != nil {
			return err
		}
		s.peerClients[peerID] = c
	}
	return nil
}

// DisconnectPeer disconnects this server from the peer identified by peerID.
func (s *Server) DisconnectPeer(peerID int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.peerClients[peerID]; ok {
		err := c.Close()
		delete(s.peerClients, peerID)
		return err
	}
	return nil
}

func (s *Server) Call(id int, serviceMethod string, args, reply interface{}) error {
	s.mu.Lock()
	peer := s.peerClients[id]
	s.mu.Unlock()

	// If this is called after shutdown (where client.Close is called), it will
	// return an error.
	if peer == nil {
		return fmt.Errorf("call client [%d] after it's closed", id)
	}
	return peer.Call(serviceMethod, args, reply)
}

// RPCProxy is a trivial pass-thru proxy type for ConsensusModule's RPC methods.
// It's useful for:
// - Simulating a small delay in RPC transmission.
// - Avoiding running into https://github.com/golang/go/issues/19957
// - Simulating possible unreliable connections by delaying some messages
//   significantly and dropping others when RAFT_UNRELIABLE_RPC is set.

// RPCProxy 是ConsensusModule中的RPC方法的直通代理类型。
// 作用如下：
//  - 模拟RPC传输过程中的微小延迟
//  - 防止出现 https://github.com/golang/go/issues/19957 所提的问题
//  - 当设置RAFT_UNRELIABLE_RPC时，通过刻意延迟某些消息并丢弃其他消息
// 	来模拟可能出现的不可靠连接。
type RPCProxy struct {
	cm *ConsensusModule
}

func (p *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			p.cm.dlogf("drop RequestVote")
			return errors.New("RPC failed")
		} else if dice == 8 {
			p.cm.dlogf("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return p.cm.RequestVote(args, reply)
}

func (p *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			p.cm.dlogf("drop AppendEntries")
			return errors.New("RPC failed")
		} else if dice == 8 {
			p.cm.dlogf("delay AppendEntries")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return p.cm.AppendEntries(args, reply)
}
