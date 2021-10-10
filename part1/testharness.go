package raft

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	rand.Seed(time.Now().UnixNano())
}

type Harness struct {
	cluster   []*Server
	connected []bool

	n int
	t *testing.T
}

// NewHarness creates a new test Harness, initialized with n servers connected
// to each other.
func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	ready := make(chan struct{})

	// Create all Servers in this cluster, assign ids and peer ids.
	for i := 0; i < n; i++ {
		peerIDs := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIDs = append(peerIDs, p)
			}
		}

		ns[i] = NewServer(i, peerIDs, ready)
		ns[i].Serve()
	}

	// Connect all peers to each other.
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				_ = ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}
	close(ready)

	return &Harness{
		cluster:   ns,
		connected: connected,
		n:         n,
		t:         t,
	}
}

// Shutdown shuts down all the servers in the harness and waits for them to
// stop running.
func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
}

// DisconnectPeer disconnects a server from all other servers in the cluster.
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()

	for i := 0; i < h.n; i++ {
		if i != id {
			err := h.cluster[id].DisconnectPeer(i)
			if err != nil {
				tlog("%v", err)
			}
		}
	}
	h.connected[id] = false
}

// ReconnectPeer connects a server to all other servers in the cluster.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for i := 0; i < h.n; i++ {
		if i != id {
			if err := h.cluster[id].ConnectToPeer(i, h.cluster[i].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[i].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// CheckSingleLeader checks that only a single server thinks it's the leader.
// Returns the leader's id and term. It retries several times if no leader is
// identified yet.
func (h *Harness) CheckSingleLeader() (leaderID, leaderTerm int) {
	for r := 0; r < 5; r++ {
		leaderID = -1
		leaderTerm = -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader {
					if leaderID < 0 {
						leaderID, leaderTerm = i, term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderID, i)
					}
				}
			}
		}
		if leaderID >= 0 {
			return leaderID, leaderTerm
		}
		time.Sleep(150 * time.Millisecond)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

// CheckNoLeader checks that no connected server considers itself the leader.
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
