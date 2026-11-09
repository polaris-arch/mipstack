package mipstack

import "context"

type tcpCloseCancellation struct {
	cancel  context.CancelFunc
	next    *tcpCloseCancellation
	claimed bool
}

// CancelOnClose arranges to call cancel when application access is fully closed
// or the protocol terminates. FIN, CloseRead, CloseWrite, and I/O deadlines alone
// do not trigger cancellation. Notification does not discard buffered reads.
//
// cancel runs synchronously, outside the connection mutex, including when this
// method is called after closure. It must only signal cancellation and must not
// wait for the connection actor or stack to finish.
//
// stop returns true if it removed the registration before closure claimed it;
// in that case cancel will not be called. A false result means that closure has
// claimed it or it was already removed. stop does not wait for cancel to finish.
func (c *TCPConn) CancelOnClose(cancel context.CancelFunc) (stop func() bool) {
	if cancel == nil {
		panic("nil cancellation function")
	}
	c.mu.Lock()
	if c.userClosed || c.terminalErr != nil {
		c.mu.Unlock()
		cancel()
		return func() bool { return false }
	}
	registration := &tcpCloseCancellation{cancel: cancel, next: c.closeCancellations}
	c.closeCancellations = registration
	c.mu.Unlock()
	return func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		if registration.claimed || registration.cancel == nil || c.userClosed || c.terminalErr != nil {
			return false
		}
		for link := &c.closeCancellations; *link != nil; link = &(*link).next {
			if *link == registration {
				*link = registration.next
				registration.next = nil
				registration.cancel = nil
				return true
			}
		}
		return false
	}
}

// The caller holds c.mu while publishing full application/protocol closure.
func (c *TCPConn) takeCloseCancellationsLocked() *tcpCloseCancellation {
	registrations := c.closeCancellations
	c.closeCancellations = nil
	for registration := registrations; registration != nil; registration = registration.next {
		registration.claimed = true
	}
	return registrations
}

func runTCPCloseCancellations(registration *tcpCloseCancellation) {
	for registration != nil {
		next := registration.next
		registration.cancel()
		// Claimed registrations cannot be changed by stop; retire references.
		registration.cancel = nil
		registration.next = nil
		registration = next
	}
}
