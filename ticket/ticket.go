package ticket

import (
	"fmt"
	"github.com/segmentio/ksuid"
	"time"
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

//
// Client sessiom
type Session struct {
	Name      string    // Optional -- only meaningful to client
	Id        string    // Generated session ID
	Src       string    // ip:port of client
	Ttl       int       // ticket ttl in ms
	Tickets   []*Ticket // tickets claimed
	Issuances []*Ticket // tickets issued for this session
	expires   time.Time
}

//
// Ticket for a resource
type Ticket struct {
	Name         string   // ticket name
	ResourceName string   // Resource we belong to
	Data         []byte   // ticket data
	Issuer       *Session // Issuer  session of ticket. Never empty
	Claimant     *Session // Session ID of ticket claimant, if there is one or empty
}

//
// Resource -- a thing that can be claimed with a ticket
type Resource struct {
	Name    string
	Tickets map[string]*Ticket
}

//
// Create a new resource
func NewResource(name string) (r *Resource) {
	r = &Resource{name, make(map[string]*Ticket)}
	return
}

//
// Create a new ticket
func NewTicket(name, resname string, issuer *Session, data []byte) (t *Ticket) {
	t = &Ticket{name, resname, data, issuer, nil}
	return
}

//
// Creae a new session
func NewSession(name, src string, ttl int) (s *Session) {
	guid := ksuid.New()
	s = &Session{Name: name, Id: guid.String(), Src: src, Ttl: ttl, Tickets: []*Ticket{}, Issuances: []*Ticket{}}
	s.refresh()
	return
}

//
// Create a new ticketd
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

//
// Manage locks, sessions and tickets
func (td *TicketD) ticketProc() {
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

//
// Start pertinent goprocs
func (td *TicketD) Start() {
	go func() {
		td.ticketProc()
	}()
	if td.snapshotPath != "" {
		td.quitSnapChan = make(chan interface{})
		go func() {
			td.snapshotProc()
		}()
	}
}

//
// Signal procs to stop using quit channels
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
			s.clearClaims()
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
}

//
// refresh session
func (s *Session) refresh() {
	s.expires = time.Now().Add(time.Millisecond * time.Duration(s.Ttl))
}

//
// Clear session claims, issuances, etc
// Used on expiration of session
func (s *Session) clearClaims() {
	for _, ticket := range s.Tickets {
		if ticket.Claimant == s {
			ticket.Claimant = nil
		}
	}
	for _, ticket := range s.Issuances {
		if ticket.Issuer == s {
			ticket.Issuer = nil
		}
	}
	// Clear out arrays
	s.Tickets = []*Ticket{}
	s.Issuances = []*Ticket{}
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
	newTick.Data = []byte{}
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
		if tk.Name == t.Name {
			oldArray[i] = t
			return oldArray
		}
	}
	return append(oldArray, t)
}

func ticketRemove(oldArray []*Ticket, t *Ticket) []*Ticket {
	for i, tk := range oldArray {
		if tk.Name == t.Name {
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
	s := NewSession(name, src, ttl)
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

//
// Close a sessio and release all tickets issued and claimed
func (td *TicketD) CloseSession(id string) (err error) {
	errChan := make(chan error)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		if s := sessions[id]; s != nil {
			td.logger.Log(3, "Closing  session %s (%s)", s.Id, s.Name)
			s.clearClaims()
			delete(sessions, id)
			errChan <- nil
		} else {
			td.logger.Log(3, "Closing session: %s not found", id)
			errChan <- fmt.Errorf("Session not found: %s", id)
		}
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

//
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

//
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

// Add a ticket for a resource
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
			r = NewResource(resource)
			resources[resource] = r
		}
		ticket := NewTicket(name, resource, sess, data)
		// If ticket exists, but issued by another session we are just going to take it over
		if oldTick := r.Tickets[name]; oldTick != nil {
			oldTick.Issuer = nil // Mark this ticket as no longer valid
			ticket.Claimant = oldTick.Claimant
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

//
// Remove a ticket for a resource
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
		delete(r.Tickets, name)
		// Remove ticket from session issuance list
		sess.Issuances = ticketRemove(sess.Issuances, tick)
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

//
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
		}
		for _, ticket := range r.Tickets {
			if ticket.Issuer != nil && (ticket.Claimant == nil || ticket.Claimant == sess) {
				ticket.Claimant = sess
				ok = true
				sess.Tickets = ticketAddOrUpdate(sess.Tickets, ticket)
				t = ticket.clone()
				break
			}
		}
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

//
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
		}
		errChan <- nil
	}
	td.ticketChan <- f
	err = <-errChan
	return
}

//
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
// Get Copies of session and resource maps

// Resources
func (td *TicketD) GetResources() (out map[string]*Resource) {
	out = make(map[string]*Resource)
	errChan := make(chan error)
	defer close(errChan)
	f := func(sessions map[string]*Session, resources map[string]*Resource) {
		for k, v := range resources {
			nr := Resource{Name: k, Tickets: make(map[string]*Ticket)}
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

// Sessions
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
