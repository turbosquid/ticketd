package http

import (
	"context"
	"github.com/stretchr/testify/require"
	"github.com/turbosquid/ticketd/ticket"
	"net/http"
	"testing"
	"time"
)

func TestServerStopStart(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	time.Sleep(1 * time.Second)
	err := stopServer(td, svr)
	r.NoError(err)
}

func TestSession(t *testing.T) {
	r := require.New(t)
	td, svr := startServer()
	defer stopServer(td, svr)
	cli := NewClient("http://localhost:8080", 1*time.Second)
	time.Sleep(100 * time.Millisecond)
	// Open a session
	sess, err := cli.OpenSession("test-1", 100)
	r.NoError(err)
	r.NotNil(sess)
	r.NotEmpty(sess.Id)
	t.Logf("received id: %s", sess.Id)
	time.Sleep(90 * time.Millisecond)
	err = sess.Refresh()
	time.Sleep(90 * time.Millisecond) // Be sure we actually refreshed
	r.NoError(err)
	ts, err := sess.Get()
	r.NoError(err)
	r.NotNil(ts)
	t.Logf("got session: %#v", ts)
	r.Equal(ts.Id, sess.Id)
	r.Equal(ts.Name, "test-1")
	r.Equal(ts.Ttl, 100)
	// Close session
	err = sess.Close()
	r.NoError(err)
}

func startServer() (td *ticket.TicketD, svr *http.Server) {
	td = ticket.NewTicketD(500, "", 0, nil)
	td.Start()
	svr = StartServer("localhost:8080", td)
	return
}

func stopServer(td *ticket.TicketD, svr *http.Server) (err error) {
	err = svr.Shutdown(context.Background())
	td.Quit()
	return
}
