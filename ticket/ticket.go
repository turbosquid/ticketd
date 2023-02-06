package ticket

import (
	"fmt"
	"log"
	"runtime/debug"
	"time"

	"github.com/segmentio/ksuid"
)

const expireDelayMs = 1000

type ticketFunc func(map[string]*Session, map[string]*Resource)

type TicketD struct {
	ticketChan       chan ticketFunc
	quitChan         chan interface{}
	quitSnapChan     chan interface{}
	expireTickTimeMs int
	snapshotInterval int
	snapshotPath     string
	logger           Logger
}

// Client session
type Session struct {
	Name      string    // Optional -- only meaningful to client
	Id        string    // Generated session ID
	Src       string    // ip:port of client
	Ttl       int       // ticket ttl in ms
	Tickets   []*Ticket // tickets claimed
	Issuances []*Ticket // tickets issued for this session
	expires   time.Time
}

// Ticket for a resource
type Ticket struct {
	Name         string   // ticket name
	ResourceName string   // Resource we belong to
	Data         []byte   // ticket data
	Issuer       *Session // Issuer  session of ticket. Never empty
	Claimant     *Session // Session ID of ticket claimant, if there is one or empty
}

// Resource -- a thing that can be claimed with a ticket
type Resource struct {
	Name    string
	IsLock  bool
	Tickets map[string]*Ticket
}

// Create a new resource
func newResource(name string, isLock bool) (r *Resource) {
	r = &Resource{name, isLock, make(map[string]*Ticket)}
	return
}

// Create a new ticket
func newTicket(name, resname string, issuer *Session, data []byte) (t *Ticket) {
	t = &Ticket{name, resname, data, issuer, nil}
	return
}

// Creae a new session
func newSession(name, src string, ttl int) (s *Session) {
	guid := ksuid.New()
	s = &Session{Name: name, Id: guid.String(), Src: src, Ttl: ttl, Tickets: []*Ticket{}, Issuances: []*Ticket{}}
	s.refresh()
	return
}

// Create a new ticketd instance. expireTickMs specifies how often to run the session expiration loop. Defaults to 1000ms. snapshotPath specifies a directory
// to write snapshots to (we will attempt to create it). If empty, no snapshotting is done. snapshotInterval specifies (in ms) how often to
// write out a snashot. Defaults to 1000ms. Finally, you can pass in your own logger. If no logger is  specified, you get a DefaultLogger (logs to console) set to
// a loglevel of 3.
func NewTicketD(expireTickMs int, snapshotPath string, snapshotInterval int, logger Logger) (td *TicketD) {
	td = &TicketD{make(chan ticketFunc), make(chan interface{}), nil,
		expireTickMs, snapshotInterval, snapshotPath, logger}
	if td.expireTickTimeMs == 0 {
		td.expireTickTimeMs = expireDelayMs
	}
	if td.snapshotInterval == 0 {
		td.snapshotInterval = 1000
	}
	if td.logger == nil {
		td.logger = &DefaultLogger{3}
	}
	return
}

// Manage locks, sessions and tickets
func (td *TicketD) ticketProc() (restart bool) {
	sessions := make(map[string]*Session)
	resources := make(map[string]*Resource)
	if td.snapshotPath != "" {
		td.logger.Log(2, "Loading snapshots from %s", td.snapshotPath)
		sessionsLoaded, resourcesLoaded, err := td.loadSnapshot(td.snapshotPath)
		if err != nil {
			td.logger.Log(1, "WARNING: Loading snapshots: %s", err.Error())
		} else {
			sessions = sessionsLoaded
			resources = resourcesLoaded
		}
	}

	// Handle panics -- print info, then exit with restart flag true
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in http  hander: %#v", r)
			log.Printf("Stack trace:\n%s", debug.Stack())
			restart = true
		}
	}()

	ticker := time.NewTicker(time.Duration(td.expireTickTimeMs) * time.Millisecond)
	td.logger.Log(2, "Ticket processing starting...")
	for {
		select {
		case _ = <-ticker.C:
			td.expireSessions(sessions, resources)
		case q := <-td.quitChan:
			if q == nil {
				td.logger.Log(2, "Received quit signal. Exiting ticket processing loop...")
				close(td.quitChan)
				return
			}
		case f := <-td.ticketChan:
			f(sessions, resources)
		}
	}
}

// Start ticketd. You have to start ticketd before using it
func (td *TicketD) Start() {
	go func() {
		for {
			if restart := td.ticketProc(); !restart {
				break
			}
		}
	}()
	if td.snapshotPath != "" {
		td.quitSnapChan = make(chan interface{})
		go func() {
			for {
				if restart := td.snapshotProc(); !restart {
					break
				}
			}
		}()
	}
}

// Stop ticketd.
func (td *TicketD) Quit() {
	if td.quitSnapChan != nil {
		td.logger.Log(2, "Signaling snapshotter to quit...")
		td.quitSnapChan <- nil
		_ = <-td.quitSnapChan
	}
	td.logger.Log(2, "Signaling ticket processor to quit...")
	td.quitChan <- nil
	_ = <-td.quitChan
}

func (td *TicketD) expireSessions(sessions map[string]*Session, resources map[string]*Resource) {
	// Expire sessions
	for id, s := range sessions {
		if s.expires.Before(time.Now()) {
			td.logger.Log(3, "Expiring session %s (%s) with timeout %ds ms", s.Id, s.Name, s.Ttl)
			s.clearClaims(resources)
			delete(sessions, id)
		}
	}
	// Remove tickets with no issuer
	for _, resource := range resources {
		for tn, tick := range resource.Tickets {
			if tick.Issuer == nil {
				delete(resource.Tickets, tn)
			}
		}
	}
	// Finally, remove resources with no tickets
	for name, resource := range resources {
		if len(resource.Tickets) == 0 {
			delete(resources, name)
		}
	}
}

// refresh session
func (s *Session) refresh() {
	s.expires = time.Now().Add(time.Millisecond * time.Duration(s.Ttl))
}

// Clear session claims, issuances, etc
// Used on expiration of session
func (s *Session) clearClaims(resources map[string]*Resource) {
	for _, ticket := range s.Tickets {
		t := fetchTicketPtr(ticket, resources) // Refresh ticket ptr -- can be out of date
		if t != nil && t.Claimant == s {
			log.Printf("Clearing session %s claim on ticket %s", s.Id, ticket.Name)
			t.Claimant = nil
		}
	}
	for _, ticket := range s.Issuances {
		t := fetchTicketPtr(ticket, resources) // Refresh ticket ptr -- can be out of date
		if t != nil && t.Issuer == s {
			log.Printf("Clearing session %s issuer  on ticket %s", s.Id, ticket.Name)
			t.Issuer = nil
		}
	}
	// Clear out arrays
	s.Tickets = []*Ticket{}
	s.Issuances = []*Ticket{}
}

// Fetch a ticket pointer
func fetchTicketPtr(in *Ticket, resources map[string]*Resource) (out *Ticket) {
	r := resources[in.ResourceName]
	if r == nil {
		return
	}
	out = r.Tickets[in.Name]
	return
}

// Clone a session
func (s *Session) clone() (out *Session) {
	newSess := *s
	out = &newSess
	out.Tickets = make([]*Ticket, len(s.Tickets))
	out.Issuances = make([]*Ticket, len(s.Issuances))
	for i, ticket := range s.Issuances {
		out.Issuances[i] = ticket.clone()
	}
	for i, ticket := range s.Tickets {
		out.Tickets[i] = ticket.clone()
	}
	return
}

// Clone a ticket
func (t *Ticket) clone() (out *Ticket) {
	newTick := *t
	if t.Data != nil {
		newTick.Data = make([]byte, len(t.Data))
		copy(newTick.Data, t.Data)
	}
	copy(newTick.Data, t.Data)
	if t.Issuer != nil {
		s := *(t.Issuer)
		s.Tickets = []*Ticket{}
		s.Issuances = []*Ticket{}
		newTick.Issuer = &s
	}
	if t.Claimant != nil {
		s := *(t.Claimant)
		s.Tickets = []*Ticket{}
		s.Issuances = []*Ticket{}
		newTick.Claimant = &s
	}
	out = &newTick
	return
}

func ticketAddOrUpdate(oldArray []*Ticket, t *Ticket) []*Ticket {
	for i, tk := range oldArray {
		if tk.Name == t.Name && tk.ResourceName == t.ResourceName {
			oldArray[i] = t
			return oldArray
		}
	}
	return append(oldArray, t)
}

func ticketRemove(oldArray []*Ticket, t *Ticket) []*Ticket {
	for i, tk := range oldArray {
		if tk.Name == t.Name && tk.ResourceName == t.ResourceName {
			oldArray[i] = oldArray[len(oldArray)-1]
			return oldArray[:len(oldArray)-1]
		}
	}
	return oldArray
}

// Public functions for sessions

// Open a new session
func (td *TicketD) OpenSession(name, src string, ttl int) (id string, err error) {
	errChan := make(chan error)
	s := newSession(name, src, ttl)
	id = s.Id
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sessions[s.Id] = s
		td.logger.Log(3, "Opened new session %s (%s)", s.Id, s.Name)
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Close a session and release all tickets issued and claimed
func (td *TicketD) CloseSession(id string) (err error) {
	errChan := make(chan error)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		if s := sessions[id]; s != nil {
			td.logger.Log(3, "Closing  session %s (%s)", s.Id, s.Name)
			s.clearClaims(resources)
			delete(sessions, id)
			errChan <- nil
		} else {
			td.logger.Log(3, "Closing session: %s not found", id)
			errChan <- fmt.Errorf("Session not found: %s (%w)", id, ErrNotFound)
		}
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Get a copy of a session
func (td *TicketD) GetSession(id string) (ret *Session, err error) {
	errChan := make(chan error)
	ret = &Session{}
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		if s := sessions[id]; s != nil {
			ret = s.clone()
			errChan <- nil
		} else {
			errChan <- fmt.Errorf("Session not found: %s (%w)", id, ErrNotFound)
		}
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Refresh session timer
func (td *TicketD) RefreshSession(id string) (err error) {
	errChan := make(chan error)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		if s := sessions[id]; s != nil {
			s.refresh()
			errChan <- nil
		} else {
			errChan <- fmt.Errorf("Session not found: %s (%w)", id, ErrNotFound)
		}
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Public functions for tickets

// Issue a ticket for a resource
func (td *TicketD) IssueTicket(sessId string, resource string, name string, data []byte) (err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		sess.refresh()
		// Create resource if it does not exist
		r := resources[resource]
		if r == nil {
			r = newResource(resource, false)
			resources[resource] = r
		} else if r.IsLock {
			errChan <- fmt.Errorf("Cannot issue a ticket on a lock resource (%s) - %w", resource, ErrResourceType)
			return
		}
		ticket := newTicket(name, resource, sess, data)
		// If ticket exists, but issued by another session we are just going to take it over
		if oldTick := r.Tickets[name]; oldTick != nil {
			oldTick.Issuer = nil // Mark this issuer  as no longer valid
			ticket.Claimant = oldTick.Claimant
		} else {
			td.logger.Log(3, "Session %s issuing ticket  %s (%s)", sess.Id, r.Name, name) // Only log on new ticket issuance
		}
		r.Tickets[name] = ticket // Set new ticket in ticket list
		// Add ticket to issuance list if it is not there already
		sess.Issuances = ticketAddOrUpdate(sess.Issuances, ticket)
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Revoke a ticket for a resource
func (td *TicketD) RevokeTicket(sessId string, resource string, name string) (err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			errChan <- fmt.Errorf("Unknown resource: %s (%w)", resource, ErrNotFound)
			return
		}
		// Get ticket -- if it exists
		tick := r.Tickets[name]
		if tick == nil {
			errChan <- fmt.Errorf("Unknown ticket for resource %s -> : %s", resource, name)
			return
		}
		// We still allow revocation of a ticket, even if issued in another session
		td.logger.Log(3, "Session %s revoking ticket  %s (%s)", sess.Id, r.Name, tick.Name)
		delete(r.Tickets, name)
		// Remove ticket from session issuance list
		sess.Issuances = ticketRemove(sess.Issuances, tick)
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Claim a ticket for a resource
// ok is true and ticket will have a copy of the ticket on success
// If the ticket is clamed, ok will be false, and ticket will be nil. err eill be nil
// On anything else, err will be set
func (td *TicketD) ClaimTicket(sessId string, resource string) (ok bool, t *Ticket, err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			// We treat a missing resource as if the ticket is already claimed
			errChan <- nil
			return
		} else if r.IsLock {
			errChan <- fmt.Errorf("Cannot claim a ticket on a lock resource (%s) - %w", resource, ErrResourceType)
			return
		}
		for _, ticket := range r.Tickets {
			if ticket.Issuer != nil && (ticket.Claimant == nil || ticket.Claimant == sess) {
				ticket.Claimant = sess
				ok = true
				sess.Tickets = ticketAddOrUpdate(sess.Tickets, ticket)
				t = ticket.clone()
				td.logger.Log(3, "Session %s claimed ticket  %s (%s)", sess.Id, r.Name, t.Name)
				break
			}
		}
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Release a ticket for a resource back to pool
func (td *TicketD) ReleaseTicket(sessId string, resource string, name string) (err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			errChan <- fmt.Errorf("Unknown resource: %s (%w)", resource, ErrNotFound)
			return
		}
		ticket := r.Tickets[name]
		if ticket != nil && ticket.Claimant == sess {
			ticket.Claimant = nil
			sess.Tickets = ticketRemove(sess.Tickets, ticket)
			td.logger.Log(3, "Session %s released ticket  %s (%s)", sess.Id, r.Name, ticket.Name)
		}
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Verify that a session holds a parituclar ticket
func (td *TicketD) HasTicket(sessId string, resource string, name string) (ok bool, err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			errChan <- fmt.Errorf("Unknown resource: %s (%w)", resource, ErrNotFound)
			return
		}
		ticket := r.Tickets[name]
		if ticket != nil && ticket.Claimant == sess {
			ok = true
		}
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

//

// Get a copy of the resources table, along with all associated tickets
func (td *TicketD) GetResources() (out map[string]*Resource) {
	out = make(map[string]*Resource)
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		for k, v := range resources {
			nr := Resource{Name: k, IsLock: v.IsLock, Tickets: make(map[string]*Ticket)}
			for tn, tick := range v.Tickets {
				nr.Tickets[tn] = tick.clone()
			}
			out[k] = &nr
		}
		errChan <- nil
	}
	td.ticketChan <- f
	_ = <-errChan
	return
}

// Lock a lockable resource. If it does not exist, it will be created. If the resource exists, but is not lockable, an error is retured.
// Returns ok==true if lock succeeds. Else you can retry
func (td *TicketD) Lock(sessId, resource string) (ok bool, err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			r = newResource(resource, true)
			resources[resource] = r
		} else if !r.IsLock {
			errChan <- fmt.Errorf("Cannot lock/unlock a non-lock  resource (%s) - %w", resource, ErrResourceType)
			return
		}
		ticket := r.Tickets[resource]
		// We should have either no tickets or a single ticket with the same name as the resource
		if len(r.Tickets) > 1 || (len(r.Tickets) == 1 && ticket == nil) {
			errChan <- fmt.Errorf("Malformed lock resource %s. More than one ticket present or wrong ticket name in resource", resource)
			return
		}
		if ticket == nil {
			ticket = newTicket(resource, resource, sess, []byte{})
			r.Tickets[resource] = ticket
			sess.Issuances = ticketAddOrUpdate(sess.Issuances, ticket)
		}
		// If the single ticket is not nil, then it must belong to us (issuer) or we can't lock it
		if ticket != nil && ticket.Issuer.Id == sess.Id {
			ok = true
			errChan <- nil
			return
		}
		// No icket, so we can claim it
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Unlock a locked resource.
func (td *TicketD) Unlock(sessId, resource string) (err error) {
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		sess := sessions[sessId]
		if sess == nil {
			errChan <- fmt.Errorf("Session not found: %s (%w)", sessId, ErrNotFound)
			return
		}
		// Get resource
		r := resources[resource]
		if r == nil {
			errChan <- fmt.Errorf("Could not find lock resource %s (%w)", resource, ErrNotFound)
			return
		} else if !r.IsLock {
			errChan <- fmt.Errorf("Cannot lock/unlock a non-lock  resource (%s) - %w", resource, ErrResourceType)
			return
		}
		ticket := r.Tickets[resource]
		// We should have either no tickets or a single ticket with the same name as the resource
		if ticket == nil {
			errChan <- fmt.Errorf("Resource %s is not locked (%w)", resource, ErrNotFound)
			return
		}
		// If the single ticket is not nil, then it must belong to us (issuer) or we can't lock it
		if ticket.Issuer.Id != sess.Id {
			errChan <- fmt.Errorf("Resource %s is locked  by another session (%w)", resource, ErrNotFound)
			return
		}
		// There is a ticket and we are the issue -- so we can delete the ticket
		ticket.Issuer = nil
		delete(r.Tickets, ticket.Name)
		sess.Issuances = ticketRemove(sess.Issuances, ticket)
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

// Get a copy of the sessions table
func (td *TicketD) GetSessions() (out map[string]*Session) {
	out = make(map[string]*Session)
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		for k, v := range sessions {
			out[k] = v.clone()
		}
		errChan <- nil
	}
	td.ticketChan <- f
	_ = <-errChan
	return
}
