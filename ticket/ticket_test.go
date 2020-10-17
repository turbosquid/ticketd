package ticket

import (
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSessiond(t *testing.T) {
	r := require.New(t)
	wg := startTicketProc()
	defer stopTicketProc(wg)
	// Create and close a session
	id, err := OpenSession("test session", "ANY", 5000)
	r.NoError(err)
	r.NotEmpty(id)
	t.Logf("Session id: %s", id)
	time.Sleep(3 * time.Second) // Be sure we do not get expired
	err = CloseSession(id)
	r.NoError(err)
	// Verify that session no longer exists
	err = RefreshSession(id)
	r.Error(err)
	t.Logf("Got excpected error on refresh: %s", err.Error())

	// Verify that session gets expired as expected
	id, err = OpenSession("test session", "ANY", 1000)
	r.NoError(err)
	r.NotEmpty(id)
	err = RefreshSession(id)
	r.NoError(err)
	time.Sleep(3 * time.Second)
	err = RefreshSession(id)
	r.Error(err)
	t.Logf("Got excpected error on expired session refresh: %s", err.Error())
}

func stopTicketProc(wg *sync.WaitGroup) {
	ticketChan <- nil
	(*wg).Wait()
}

func startTicketProc() *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		Run()
	}()
	return &wg
}
