package ticket

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func TestSessiond(t *testing.T) {
	r := require.New(t)
	td, wg := startTicketD(false)
	defer stopTicketD(td, wg)
	// Create and close a session
	id, err := td.OpenSession("test session", "ANY", 5000)
	r.NoError(err)
	r.NotEmpty(id)
	t.Logf("Session id: %s", id)
	// Get a copy of the  session
	sess, err := td.GetSession(id)
	r.NoError(err)
	r.NotNil(sess)
	r.Equal("test session", sess.Name)
	r.Equal("ANY", sess.Src)
	r.Equal(5000, sess.Ttl)
	err = td.CloseSession(id)
	r.NoError(err)
	// Verify that session no longer exists
	err = td.RefreshSession(id)
	r.Error(err)
	t.Logf("Got excpected error on refresh: %s", err.Error())

	// Verify that session gets expired as expected
	id, err = td.OpenSession("test session", "ANY", 500)
	r.NoError(err)
	r.NotEmpty(id)
	err = td.RefreshSession(id)
	r.NoError(err)
	time.Sleep(2 * time.Second)
	err = td.RefreshSession(id)
	r.Error(err)
	t.Logf("Got excpected error on expired session refresh: %s", err.Error())
}

func TestTicketIssue(t *testing.T) {
	r := require.New(t)
	td, wg := startTicketD(false)
	defer stopTicketD(td, wg)
	// Create and close a session
	issuerId, err := td.OpenSession("test issuer", "ANY", 1000)
	r.NoError(err)
	claimant1Id, err := td.OpenSession("test claimant 1", "ANY", 1000)
	r.NoError(err)
	claimant2Id, err := td.OpenSession("test claimant 2", "ANY", 1000)
	r.NoError(err)
	claimant3Id, err := td.OpenSession("test claimant 3", "ANY", 1000)
	r.NoError(err)
	err = td.IssueTicket(issuerId, "test", "foo", []byte("test foo data"))
	r.NoError(err)
	err = td.IssueTicket(issuerId, "test", "bar", []byte("test bar data"))
	r.NoError(err)
	// Ensure a bogus session cannot claim a ticket
	ok, ticket0, err := td.ClaimTicket("BADID", "test")
	r.False(ok)
	r.Nil(ticket0)
	r.Error(err)
	// Ensure we can claim tickets
	ok, ticket1, err := td.ClaimTicket(claimant1Id, "test")
	r.True(ok)
	r.NotNil(ticket1)
	r.NoError(err)
	t.Logf("ticket1: %s, %s", ticket1.Name, string(ticket1.Data))
	ok, ticket2, err := td.ClaimTicket(claimant2Id, "test")
	r.True(ok)
	r.NotNil(ticket2)
	r.NoError(err)
	t.Logf("ticket2: %s, %s", ticket2.Name, string(ticket2.Data))
	// Ensure a session can reclaim a ticket they already have
	ok, ticket3, err := td.ClaimTicket(claimant2Id, "test")
	r.True(ok)
	r.NotNil(ticket3)
	r.NoError(err)
	r.Equal(ticket2, ticket3)
	t.Logf("ticket3: %s, %s", ticket3.Name, string(ticket3.Data))
	// Test no ticket available
	ok, ticket4, err := td.ClaimTicket(claimant3Id, "test")
	r.False(ok)
	r.NoError(err)
	r.Nil(ticket4)
	dumpResources(t, td, nil)
	dumpSessions(t, td, nil)
	// Release a ticket and see if claimant3 now gets one
	err = td.ReleaseTicket(claimant1Id, "test", ticket1.Name)
	r.NoError(err)
	dumpResources(t, td, nil)
	ok, ticket4, err = td.ClaimTicket(claimant3Id, "test")
	r.True(ok)
	r.NoError(err)
	r.NotNil(ticket4)
	r.Equal(ticket1.Name, ticket4.Name) // Verify we got released ticket
	// Revoke tickets and ensure none are available to claim
	r.NoError(td.RevokeTicket(issuerId, "test", "foo"))
	r.NoError(td.RevokeTicket(issuerId, "test", "bar"))
	ok, ticket5, err := td.ClaimTicket(claimant3Id, "test")
	r.False(ok)
	r.NoError(err)
	r.Nil(ticket5)
	// Ensure error if we claim a ticket for a non-existent resource
	ok, ticket5, err = td.ClaimTicket(claimant3Id, "invalid-resource")
	r.False(ok)
	r.Error(err)
	r.Nil(ticket5)
	time.Sleep(1 * time.Second)
}

func TestIssuerTimeout(t *testing.T) {
	r := require.New(t)
	td, wg := startTicketD(false)
	defer stopTicketD(td, wg)
	// Create a session, issue a ticket and let it expire
	issuerId, err := td.OpenSession("test issuer", "ANY", 500)
	r.NoError(err)
	err = td.IssueTicket(issuerId, "test", "foo", []byte("test foo data"))
	r.NoError(err)
	time.Sleep(1 * time.Second)
	claimant1Id, err := td.OpenSession("test claimant 1", "ANY", 1000)
	r.NoError(err)
	ok, ticket1, err := td.ClaimTicket(claimant1Id, "test")
	r.False(ok)
	r.Nil(ticket1)
	r.NoError(err)
}

func TestSnapshot(t *testing.T) {
	r := require.New(t)
	td, wg := startTicketD(false)
	stopped := false
	defer func() {
		if !stopped {
			stopTicketD(td, wg)
		}
	}()
	// Create bunch of sessions and tickets
	for i := 0; i < 10; i++ {
		id, err := td.OpenSession(fmt.Sprintf("issuer %d", i), "ANY", 5000)
		r.NoError(err)
		err = td.IssueTicket(id, "test", fmt.Sprintf("ticket %d", i), []byte{})
		r.NoError(err)
	}
	// Create 10 claimant sessions to claim tickets
	for i := 0; i < 10; i++ {
		id, err := td.OpenSession(fmt.Sprintf("claimant %d", i), "ANY", 5000)
		r.NoError(err)
		ok, _, err := td.ClaimTicket(id, "test")
		r.NoError(err)
		r.True(ok)
	}
	// Compare sessions/resources
	sessions := td.GetSessions()
	err := snapshotSessions("./snaps", sessions)
	r.NoError(err)
	resources := td.GetResources()
	err = snapshotResources("./snaps", resources)
	r.NoError(err)
	lsess, lres, err := td.LoadSnapshot("./snaps")
	r.NoError(err)
	dumpSessions(t, td, lsess)
	dumpResources(t, td, lres)
	stopTicketD(td, wg)
	stopped = true
}

func dumpResources(t *testing.T, td *TicketD, resources map[string]*Resource) {
	if resources == nil {
		resources = td.GetResources()
	}
	t.Logf("Dumping resource table...")
	for _, rv := range resources {
		t.Logf("resource: %s", rv.Name)
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

func dumpSessions(t *testing.T, td *TicketD, sessions map[string]*Session) {
	if sessions == nil {
		sessions = td.GetSessions()
	}
	t.Logf("Dumping session table...")
	for _, s := range sessions {
		t.Logf("sess: %s %s %s %d ms", s.Name, s.Id, s.Src, s.Ttl)
		t.Logf("  Claims: ")
		for _, tick := range s.Tickets {
			t.Logf("    ticket: %s", tick.Name)
			t.Logf("        issuer: %s (%s)", tick.Issuer.Id, tick.Issuer.Name)
			if tick.Claimant != nil {
				t.Logf("        claimant: %s (%s)", tick.Claimant.Id, tick.Claimant.Name)
			}
		}
		t.Logf("  Issuances: ")
		for _, tick := range s.Issuances {
			t.Logf("    ticket: %s", tick.Name)
			t.Logf("        issuer: %s (%s)", tick.Issuer.Id, tick.Issuer.Name)
			if tick.Claimant != nil {
				t.Logf("        claimant: %s (%s)", tick.Claimant.Id, tick.Claimant.Name)
			}
		}
	}
	t.Logf("== END ==")
}

func stopTicketD(td *TicketD, wg *sync.WaitGroup) {
	td.Quit()
	(*wg).Wait()
}

func startTicketD(snap bool) (*TicketD, *sync.WaitGroup) {
	td := NewTicketD(500)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		td.Run()
	}()
	if snap {
		wg.Add(1)
		go func() {
			defer wg.Done()
			td.Snapshot(500, "./snaps")
		}()
	}
	return td, &wg
}
