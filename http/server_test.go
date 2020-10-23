package http

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/turbosquid/ticketd/ticket"
	"net/http"
	"testing"
	"time"
)

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
	sess, code, err := cli.OpenSession("test-1", 100)
	r.NoError(err)
	r.Zero(code)
	r.NotNil(sess)
	r.NotEmpty(sess.Id)
	t.Logf("received id: %s", sess.Id)
	time.Sleep(90 * time.Millisecond)
	code, err = sess.Refresh()
	r.NoError(err)
	r.Zero(code)
	time.Sleep(90 * time.Millisecond) // Be sure we actually refreshed
	ts, code, err := sess.Get()
	r.NoError(err)
	r.NotNil(ts)
	r.Zero(code)
	t.Logf("got session: %#v", ts)
	r.Equal(ts.Id, sess.Id)
	r.Equal(ts.Name, "test-1")
	r.Equal(ts.Ttl, 100)
	// Close session
	code, err = sess.Close()
	r.NoError(err)
	r.Zero(code)
	// Test session not found
	code, err = sess.Refresh()
	r.Error(err)
	r.Equal(404, code)
	t.Logf("Got expected error %s", err.Error())
	ts, code, err = sess.Get()
	r.Error(err)
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
	sess, _, err := cli.OpenSession("test-1", 500)
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
	_, err = sess.Close()
	r.NoError(err)
	<-notChan
	// Test session heartbeat when session has expired
	sess, _, err = cli.OpenSession("test-2", 200)
	r.NoError(err)
	sess.RunHeartbeat(300*time.Millisecond, 100*time.Millisecond, true, f)
	time.Sleep(2 * time.Second)
	r.Error(hbErr)
	<-notChan
	// Verify a 404 on clode
	code, err := sess.Close()
	r.Error(err)
	r.Equal(404, code)
	// Verify a 404 on session get
	_, code, err = sess.Get()
	r.Error(err)
	r.Equal(404, code)
	// Test heartbeat session failure when connection is lost from service
	sess, _, err = cli.OpenSession("test-3", 500)
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
	issuer, _, err := cli.OpenSession("issuer", 100)
	r.NoError(err)
	claimant, _, err := cli.OpenSession("claimant", 100)
	r.NoError(err)
	code, err := issuer.IssueTicket("test", "ticket-1", []byte("FOO"))
	r.NoError(err)
	r.Empty(code)
	ok, ticket, code, err := claimant.ClaimTicket("test")
	r.NoError(err)
	r.True(ok)
	r.NotNil(ticket)
	r.Zero(code)
	t.Logf("Got ticket: %#v", ticket)
	t.Logf("    Issuer: %#v", ticket.Issuer)
	t.Logf("    Claimant: %#v", ticket.Claimant)
	r.Equal(ticket.Name, "ticket-1")
	r.Equal(ticket.ResourceName, "test")
	r.Equal(ticket.Data, []byte("FOO"))

}

func startServer() (td *ticket.TicketD, svr *http.Server) {
	td = ticket.NewTicketD(500, "", 0, &ticket.DefaultLogger{1})
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
