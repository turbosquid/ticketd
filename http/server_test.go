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
