package ticket

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestLocks(t *testing.T) {
	r := require.New(t)
	td := startTicketD(false)
	defer stopTicketD(td)
	sessId1, err := td.OpenSession("session-1", "ANY", 100)
	r.NoError(err)
	sessId2, err := td.OpenSession("session-2", "ANY", 100)
	r.NoError(err)
	// New lock
	ok, err := td.Lock(sessId1, "/foo/bar")
	r.NoError(err)
	r.True(ok)
	// Retry lock we already hold
	ok, err = td.Lock(sessId1, "/foo/bar")
	r.NoError(err)
	r.True(ok)
	dumpResources(t, td, td.GetResources())
	// Try to claim held lock
	ok, err = td.Lock(sessId2, "/foo/bar")
	r.NoError(err)
	r.False(ok)
	// Unlock
	err = td.Unlock(sessId1, "/foo/bar")
	r.NoError(err)
	// Try to claim free  lock
	ok, err = td.Lock(sessId2, "/foo/bar")
	r.NoError(err)
	r.True(ok)
	// Try to ulock lock we do not own
	err = td.Unlock(sessId1, "/foo/bar")
	r.Error(err)
	// Check expiration logic
	time.Sleep(600 * time.Millisecond)
	r.Empty(td.GetResources()) // Resorces should be tidied up

}
