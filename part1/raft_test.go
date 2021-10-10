package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	originLeaderID, originTerm := h.CheckSingleLeader()

	h.DisconnectPeer(originLeaderID)
	sleepMs(350)

	newLeaderID, newTerm := h.CheckSingleLeader()

	if newLeaderID == originLeaderID {
		t.Errorf("want new leader to be different from orig leader")
	}
	if newTerm <= originTerm {
		t.Errorf("want newTerm <= origTerm, got %d and %d", newTerm, originTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderId, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderId)
	otherId := (origLeaderId + 1) % 3
	h.DisconnectPeer(otherId)

	// No quorum.
	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect one other server; now we'll have quorum.
	h.ReconnectPeer(otherId)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)
	// Disconnect all servers from the start. There will be no leader.
	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}

	sleepMs(450)
	h.CheckNoLeader()

	// Reconnect all servers. A leader will be found.
	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderID, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderID)
	sleepMs(350)

	newLeaderID, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderID)
	sleepMs(450)

	againLeaderID, againTerm := h.CheckSingleLeader()

	if againLeaderID != newLeaderID {
		t.Errorf("again leader id got %d; want %d", againLeaderID, newLeaderID)
	}

	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, againLeaderID)
	}
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	origLeaderID, _ := h.CheckSingleLeader()

	h.DisconnectPeer(origLeaderID)
	sleepMs(150)

	newLeaderID, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(origLeaderID)
	sleepMs(150)

	againLeaderID, againTerm := h.CheckSingleLeader()

	if newLeaderID != againLeaderID {
		t.Errorf("again leader id got %d; want %d", againLeaderID, newLeaderID)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	origLeaderID, origTerm := h.CheckSingleLeader()
	otherID := (origLeaderID + 1) % 3

	h.DisconnectPeer(otherID)
	sleepMs(650)

	h.ReconnectPeer(otherID)
	sleepMs(150)

	// We can't have an assertion on the new leader id here because it depends
	// on the relative election timeouts. We can assert that the term changed,
	// however, which implies that re-election has occurred.
	_, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Errorf("newTerm=%d, origTerm=%d", newTerm, origTerm)
	}

}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderID, _ := h.CheckSingleLeader()
		h.DisconnectPeer(leaderID)

		otherID := (leaderID + 1) % 3
		h.DisconnectPeer(otherID)

		sleepMs(310)
		h.CheckNoLeader()

		// Reconnect both.
		h.ReconnectPeer(otherID)
		h.ReconnectPeer(leaderID)

		// Give it time to settle
		sleepMs(150)
	}
}
