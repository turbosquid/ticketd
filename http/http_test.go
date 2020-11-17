package http

import (
	"context"
	"flag"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/turbosquid/ticketd/ticket"
	"net/http"
	"testing"
	"time"
)

var logLevel = flag.Int("loglevel", 0, "Log level to use")

func TestServerStopStart(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	time.Sleep(10 * time.Millisecond)
	err := stopServer(td, svr)
	r.NoError(err)
}

func TestSession(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	defer stopServer(td, svr)
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(10 * time.Millisecond) // We have to allow server time to start
	// Open a session
	sess, err := cli.OpenSession("test 1 2 3", 100)
	r.NoError(err)
	r.NotNil(sess)
	r.NotEmpty(sess.Id)
	t.Logf("received id: %s", sess.Id)
	time.Sleep(90 * time.Millisecond)
	err = sess.Refresh()
	r.NoError(err)
	time.Sleep(90 * time.Millisecond) // Be sure we actually refreshed
	ts, err := sess.Get()
	r.NoError(err)
	r.NotNil(ts)

	t.Logf("got session: %#v", ts)
	r.Equal(ts.Id, sess.Id)
	r.Equal(ts.Name, "test 1 2 3")
	r.Equal(ts.Ttl, 100)
	// Close session
	err = sess.Close()
	r.NoError(err)

	// Test session not found
	err = sess.Refresh()
	r.Error(err)
	code := HttpErrorCode(err)
	r.Equal(404, code)
	t.Logf("Got expected error %s", err.Error())
	ts, err = sess.Get()
	r.Error(err)
	code = HttpErrorCode(err)
	r.Equal(404, code)
}

func TestSessionHeartBeat(t *testing.T) {
	r := require.New(t)
	stopped := false
	td, svr := startServer()
	defer func() {
		if !stopped {
			stopServer(td, svr)
		}
	}()
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(10 * time.Millisecond) // We have to allow server time to start
	// Open a session
	sess, err := cli.OpenSession("test-1", 500)
	r.NoError(err)
	notChan := make(chan interface{})
	var hbErr error
	f := func(err error) {
		if err == nil {
			fmt.Printf("Heartbeat exits normally\n")
		} else {
			fmt.Printf("Heartbeat error: %s \n", err.Error())
			hbErr = err
		}
		notChan <- nil
	}
	sess.RunHeartbeat(300*time.Millisecond, 100*time.Millisecond, false, f)
	time.Sleep(2 * time.Second)
	// No error on close proves session is still valid after 2 seconds
	err = sess.Close()
	r.NoError(err)
	<-notChan
	// Test session heartbeat when session has expired
	sess, err = cli.OpenSession("test-2", 200)
	r.NoError(err)
	sess.RunHeartbeat(300*time.Millisecond, 100*time.Millisecond, true, f)
	time.Sleep(2 * time.Second)
	r.Error(hbErr)
	<-notChan
	// Verify a 404 on clode
	err = sess.Close()
	r.Error(err)
	code := HttpErrorCode(err)
	r.Equal(404, code)
	// Verify a 404 on session get
	_, err = sess.Get()
	r.Error(err)
	code = HttpErrorCode(err)
	r.Equal(404, code)
	// Test heartbeat session failure when connection is lost from service
	sess, err = cli.OpenSession("test-3", 500)
	r.NoError(err)
	sess.RunHeartbeat(100*time.Millisecond, 100*time.Millisecond, false, f)
	time.Sleep(2 * time.Second)
	stopServer(td, svr)
	r.Error(hbErr)
	<-notChan
	stopped = true
}

func TestTickets(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	defer stopServer(td, svr)
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(10 * time.Millisecond) // We have to allow server time to start
	// Open a session
	issuer, err := cli.OpenSession("issuer", 100)
	r.NoError(err)
	claimant, err := cli.OpenSession("claimant", 100)
	r.NoError(err)
	claimant2, err := cli.OpenSession("claimant2", 100)
	r.NoError(err)
	//Issue a ticket
	err = issuer.IssueTicket("test", "ticket 1", []byte("FOO"))
	r.NoError(err)
	// Claim ticket
	ok, ticket, err := claimant.ClaimTicket("test")
	r.NoError(err)
	r.True(ok)
	r.NotNil(ticket)

	r.Equal(ticket.Name, "ticket 1")
	r.Equal(ticket.ResourceName, "test")
	r.Equal(ticket.Data, []byte("FOO"))
	r.Equal(ticket.Claimant.Name, "claimant")
	r.Equal(ticket.Claimant.Id, claimant.Id)
	r.Equal(ticket.Issuer.Name, "issuer")
	r.Equal(ticket.Issuer.Id, issuer.Id)
	// Verify that we have ticket
	ok, err = claimant.HasTicket("test", ticket.Name)
	r.NoError(err)

	r.True(ok)
	// Verify that THIS guy does not
	ok, err = claimant2.HasTicket("test", ticket.Name)
	r.NoError(err)

	r.False(ok)
	// Release ricket
	err = claimant.ReleaseTicket("test", ticket.Name)
	r.NoError(err)
	ticket = nil
	// Verify that climant 2 can pick it up
	ok, ticket, err = claimant2.ClaimTicket("test")
	r.NoError(err)
	r.True(ok)
	r.NotNil(ticket)
	// Revoke ticket
	err = issuer.RevokeTicket("test", "ticket 1")

	r.NoError(err)
	// Verify thst claimant2 no longer hs ticket
	ok, err = claimant2.HasTicket("test", "ticket 1")
	r.NoError(err)

	r.False(ok)
	// Verify tht ticket cannot be claied
	ok, ticket, err = claimant.ClaimTicket("test")
	r.NoError(err)
	r.False(ok)
	r.Nil(ticket)
}

func TestLocks(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	defer stopServer(td, svr)
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(10 * time.Millisecond) // We have to allow server time to start
	// Open a session
	session1, err := cli.OpenSession("session1", 100)
	r.NoError(err)
	session2, err := cli.OpenSession("session2", 100)
	r.NoError(err)
	ok, err := session1.Lock("foo.bar")
	r.NoError(err)
	r.True(ok)

	ok, err = session2.Lock("foo.bar")
	r.NoError(err)
	r.False(ok)

	err = session1.Unlock("foo.bar")
	r.NoError(err)

	ok, err = session2.Lock("foo.bar")
	r.NoError(err)
	r.True(ok)
}

func TestDump(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	defer stopServer(td, svr)
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(10 * time.Millisecond) // We have to allow server time to start
	// Open sessions
	session1, err := cli.OpenSession("session1", 100)
	r.NoError(err)
	r.NotNil(session1)
	session2, err := cli.OpenSession("session2", 100)
	r.NoError(err)
	r.NotNil(session2)
	sessions, err := cli.GetSessions()
	r.NoError(err)
	r.NotNil(sessions)
	r.Equal(2, len(sessions))
	session1.IssueTicket("test", "ticket-1", []byte("FOO"))
	session1.IssueTicket("test", "ticket-2", []byte("FOO"))
	session2.IssueTicket("test2", "ticket-1", []byte("FOO"))
	session2.IssueTicket("test2", "ticket-2", []byte("FOO"))
	session1.IssueTicket("test2", "ticket-s31", []byte("FOO"))
	resources, err := cli.GetResources("")
	r.NoError(err)
	r.NotNil(resources)
	r.Equal(2, len(resources))
	r.NotNil(resources["test"])
	r.NotNil(resources["test2"])
	r.Equal(2, len(resources["test"].Tickets))
	r.Equal(3, len(resources["test2"].Tickets))
	// Dump a specific resource
	resource, err := cli.GetResources("test2")
	r.NoError(err)
	r.NotNil(resource)
	r.NotNil(resource["test2"])
	r.Equal(1, len(resource))
	r.Equal(3, len(resource["test2"].Tickets))
	dumpSessions(t, sessions)
	dumpResources(t, resources)
}

func startServer() (td *ticket.TicketD, svr *http.Server) {
	DebugFlag(true)
	td = ticket.NewTicketD(500, "", 0, &ticket.DefaultLogger{*logLevel})
	td.Start()
	svr = StartServer("localhost:8080", td)
	return
}

func stopServer(td *ticket.TicketD, svr *http.Server) (err error) {
	ctx := context.Background()
	err = svr.Shutdown(ctx)
	td.Quit()
	return
}

func dumpSessions(t *testing.T, sessions map[string]*ticket.Session) {
	t.Logf("Dumping session table...")
	for _, s := range sessions {
		t.Logf("sess: %s %s %s %d ms", s.Name, s.Id, s.Src, s.Ttl)
	}
	t.Logf("== END ==")
}

func dumpResources(t *testing.T, resources map[string]*ticket.Resource) {
	t.Logf("Dumping resource table...")
	for _, rv := range resources {
		t.Logf("resource: %s  isLock: %t", rv.Name, rv.IsLock)
		for _, tick := range rv.Tickets {
			t.Logf("    ticket: %s", tick.Name)
			t.Logf("        issuer: %s (%s)", tick.Issuer.Id, tick.Issuer.Name)
			if tick.Claimant != nil {
				t.Logf("        claimant: %s (%s)", tick.Claimant.Id, tick.Claimant.Name)
			}
		}
	}
	t.Logf("== END ==")
}
