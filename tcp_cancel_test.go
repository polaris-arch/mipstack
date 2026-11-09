package mipstack

import (
	"context"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTCPCancelOnCloseStopAndClaim(t *testing.T) {
	connection := &TCPConn{}
	var calls atomic.Int32
	stop := connection.CancelOnClose(func() { calls.Add(1) })
	if !stop() || stop() || connection.closeCancellations != nil {
		t.Fatal("stop did not remove the registration exactly once")
	}
	stop = connection.CancelOnClose(func() { calls.Add(1) })
	connection.mu.Lock()
	connection.terminalErr = net.ErrClosed
	registrations := connection.takeCloseCancellationsLocked()
	connection.mu.Unlock()
	// Claim is authoritative even before the CancelFunc has run.
	if stop() || calls.Load() != 0 {
		t.Fatal("a claimed cancellation was accepted as a successful stop")
	}
	runTCPCloseCancellations(registrations)
	if calls.Load() != 1 || registrations.cancel != nil || registrations.next != nil {
		t.Fatal("claimed cancellation did not retire its references")
	}
	if connection.CancelOnClose(func() { calls.Add(1) })() || calls.Load() != 2 {
		t.Fatal("registration after termination missed cancellation")
	}
}

func TestTCPCancelOnCloseConcurrentRegistration(t *testing.T) {
	for i := 0; i < 100; i++ {
		connection := &TCPConn{}
		var calls atomic.Int32
		var stopped bool
		var group sync.WaitGroup
		group.Add(2)
		go func() {
			defer group.Done()
			stop := connection.CancelOnClose(func() { calls.Add(1) })
			stopped = stop()
		}()
		go func() {
			defer group.Done()
			connection.mu.Lock()
			connection.terminalErr = net.ErrClosed
			registrations := connection.takeCloseCancellationsLocked()
			connection.mu.Unlock()
			runTCPCloseCancellations(registrations)
		}()
		group.Wait()
		if calls.Load() > 1 || connection.closeCancellations != nil {
			t.Fatal("concurrent registration leaked or notified twice")
		}
		if (calls.Load() == 0) != stopped {
			t.Fatal("termination lost a registration without a successful stop")
		}
	}
}

func TestTCPCancelOnCloseResetPreservesQueuedReads(t *testing.T) {
	link, _, connection := newManuallyPumpedTCPConnection(t)
	defer connection.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := connection.CancelOnClose(cancel)
	link.mu.Lock()
	peer := link.tcp[connection.key.local.Port()]
	sequence, acknowledgement := peer.serverNext, peer.clientNext
	link.mu.Unlock()
	payload := []byte("queued before reset")
	if err := link.deliverTCP(connection.key.remote.Port(), connection.key.local.Port(), sequence, acknowledgement, TCPFlagACK, 65535, nil, payload); err != nil {
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		connection.mu.Lock()
		defer connection.mu.Unlock()
		return connection.readBuffer.size == len(payload)
	})
	if err := link.deliverTCP(connection.key.remote.Port(), connection.key.local.Port(), sequence+uint32(len(payload)), acknowledgement, TCPFlagRST, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("reset did not cancel pending external work")
	}
	if stop() {
		t.Fatal("reset cancellation lost ownership")
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(connection, got); err != nil || string(got) != string(payload) {
		t.Fatalf("cancellation discarded queued reads: %q, %v", got, err)
	}
}

func TestTCPCancelOnCloseHalfCloseAndDeadlines(t *testing.T) {
	_, _, connection := newManuallyPumpedTCPConnection(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connection.CancelOnClose(cancel)
	for _, action := range []func() error{
		func() error { return connection.SetDeadline(time.Now().Add(-time.Second)) },
		func() error { return connection.SetDeadline(time.Time{}) },
		connection.CloseRead,
		connection.CloseWrite,
	} {
		if err := action(); err != nil {
			t.Fatal(err)
		}
		if ctx.Err() != nil {
			t.Fatal("half-close/deadline cancelled external work")
		}
	}
	if err := connection.Close(); err != nil || ctx.Err() == nil {
		t.Fatalf("full close did not cancel: %v, %v", err, ctx.Err())
	}
}

// Both endpoints run the real stack. A peer FIN completes reads, not the
// connection lifetime: a registered preparation must still be able to respond.
func TestTCPCancelOnClosePeerFINKeepsResponse(t *testing.T) {
	clientAddress := netip.MustParseAddr("192.0.2.10")
	serverAddress := netip.MustParseAddr("192.0.2.20")
	client := newForwarderTestStack(t, clientAddress, false)
	server := newForwarderTestStack(t, serverAddress, false)
	newStackBridge(t, client, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	listener, err := server.ListenTCP(ctx, "tcp4", netip.AddrPortFrom(serverAddress, 8080))
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_ = listener.(*TCPListener).SetDeadline(time.Now().Add(2 * time.Second))
	peer, err := client.DialTCP(ctx, "tcp4", netip.AddrPort{}, netip.AddrPortFrom(serverAddress, 8080))
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	connection := accepted.(*TCPConn)
	defer connection.Close()
	_ = peer.SetDeadline(time.Now().Add(2 * time.Second))
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	var calls atomic.Int32
	stop := connection.CancelOnClose(func() { calls.Add(1) })
	defer stop()
	if _, err = peer.Write([]byte("request")); err != nil {
		t.Fatal(err)
	}
	if err = peer.(*TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	request, err := io.ReadAll(connection)
	if err != nil || string(request) != "request" {
		t.Fatalf("request before peer FIN = %q, %v", request, err)
	}
	_ = connection.Info() // Actor barrier after the FIN, not a sleep-based check.
	if calls.Load() != 0 {
		t.Fatal("peer FIN canceled a live registration")
	}
	if _, err = connection.Write([]byte("response")); err != nil {
		t.Fatal(err)
	}
	response := make([]byte, len("response"))
	if _, err = io.ReadFull(peer, response); err != nil || string(response) != "response" {
		t.Fatalf("response after peer FIN = %q, %v", response, err)
	}
	if calls.Load() != 0 {
		t.Fatal("response direction canceled before full closure")
	}
	if err = connection.Close(); err != nil || calls.Load() != 1 {
		t.Fatalf("full close = %v, cancellation calls = %d", err, calls.Load())
	}
}

func TestTCPCancelOnCloseBeforeLingerAndStackShutdown(t *testing.T) {
	for _, method := range []string{"linger", "stack"} {
		t.Run(method, func(t *testing.T) {
			_, stack, connection := newManuallyPumpedTCPConnection(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			connection.CancelOnClose(cancel)
			done := make(chan struct{})
			if method == "linger" {
				if err := connection.SetLinger(2); err != nil {
					t.Fatal(err)
				}
				go func() { _ = connection.Close(); close(done) }()
			} else {
				go func() { _ = stack.Close(); close(done) }()
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("close did not promptly signal cancellation")
			}
			if method == "linger" {
				select {
				case <-done:
					t.Fatal("fixture did not wait for positive linger")
				default:
				}
				_ = stack.Close()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("close did not finish")
			}
		})
	}
}
