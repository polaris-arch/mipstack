package mipstack

import (
	"net"
	"testing"
)

// Exercise independent registrations, unlinking a middle owner, and claim
// authority without inspecting a particular registration-state representation.
func TestTCPCancelOnCloseIndependentRegistrations(t *testing.T) {
	c := &TCPConn{}
	calls := [3]int{}
	stops := make([]func() bool, 3)
	for i := range stops {
		i := i
		stops[i] = c.CancelOnClose(func() { calls[i]++ })
	}
	if !stops[1]() || stops[1]() {
		t.Fatal("middle owner did not detach exactly once")
	}
	c.mu.Lock()
	c.terminalErr = net.ErrClosed
	pending := c.takeCloseCancellationsLocked()
	c.mu.Unlock()
	if stops[0]() || stops[2]() {
		t.Fatal("claim lost before callback execution")
	}
	runTCPCloseCancellations(pending)
	if calls != [3]int{1, 0, 1} {
		t.Fatal("lost or repeated notification", calls)
	}
}
func TestTCPCancelOnCloseStopDoesNotRemoveReplacement(t *testing.T) {
	c := &TCPConn{}
	first := c.CancelOnClose(func() { t.Error("detached owner called") })
	if !first() {
		t.Fatal("first owner did not stop")
	}
	second := c.CancelOnClose(func() {})
	if first() || !second() {
		t.Fatal("old token disturbed replacement")
	}
}
func TestTCPCancelOnCloseCallbackOutsideMutex(t *testing.T) {
	c := &TCPConn{}
	calls := 0
	stop := c.CancelOnClose(func() {
		c.mu.Lock()
		closed := c.terminalErr != nil
		c.mu.Unlock()
		if !closed {
			t.Error("terminal state not published")
		}
		c.CancelOnClose(func() { calls++ })
		calls++
	})
	c.mu.Lock()
	c.terminalErr = net.ErrClosed
	pending := c.takeCloseCancellationsLocked()
	c.mu.Unlock()
	runTCPCloseCancellations(pending)
	if stop() || calls != 2 {
		t.Fatal("callback/late-registration contract", calls)
	}
}
