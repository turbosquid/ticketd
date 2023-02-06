package ticket

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"
)

// Load snapshot from disk if it exists
func (td *TicketD) loadSnapshot(path string) (sessions map[string]*Session, resources map[string]*Resource, err error) {

	sessions, err = loadSessions(path)
	if err != nil {
		return
	}
	resources, err = loadResources(path)
	if err != nil {
		return
	}
	// Now we have to fix up a lot of pointers
	for _, sess := range sessions {
		if sess.Issuances == nil {
			sess.Issuances = []*Ticket{}
		}
		if sess.Tickets == nil {
			sess.Tickets = []*Ticket{}
		}
	}
	for _, sess := range sessions {
		for i, ticket := range sess.Tickets {
			// Be sure the things we THINK exist exist in resources
			res := resources[ticket.ResourceName]
			if res == nil {
				return nil, nil, fmt.Errorf("unable to find resource %s", ticket.ResourceName)
			}
			// Replace ticket pointer here to pointer from resources
			sess.Tickets[i] = res.Tickets[ticket.Name]
			if sess.Tickets[i] == nil {
				return nil, nil, fmt.Errorf("ticket %s does not exist for resource %s", ticket.Name, res.Name)
			}
			// Updateticket claimant pointer to THIS session pointer
			sess.Tickets[i].Claimant = sess
		}
		for i, ticket := range sess.Issuances {
			// Be sure the things we THINK exist exist in resources
			res := resources[ticket.ResourceName]
			if res == nil {
				return nil, nil, fmt.Errorf("unable to find resource %s", ticket.ResourceName)
			}
			// Replace ticket pointer here to pointer from resources
			sess.Issuances[i] = res.Tickets[ticket.Name]
			if sess.Issuances[i] == nil {
				return nil, nil, fmt.Errorf("ticket %s does not exist for resource %s", ticket.Name, res.Name)
			}
			sess.Issuances[i].Issuer = sess
		}
		sess.refresh()
	}
	for _, res := range resources {
		for _, ticket := range res.Tickets {
			if ticket.Data == nil {
				ticket.Data = []byte{}
			}
			if ticket.Issuer != nil {
				ticket.Issuer = sessions[ticket.Issuer.Id]
			}
			if ticket.Claimant != nil {
				ticket.Claimant = sessions[ticket.Claimant.Id]
			}
		}
	}
	return
}

// Optional snapshot loop
func (td *TicketD) snapshotProc() (restart bool) {
	ticker := time.NewTicker(time.Duration(td.snapshotInterval) * time.Millisecond)
	td.logger.Log(2, "Snapshot loop starting...")
	os.MkdirAll(td.snapshotPath, 0755)
	// Handle panics -- print info, then exit with restart flag true
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC in http  hander: %#v", r)
			log.Printf("Stack trace:\n%s", debug.Stack())
			restart = true
		}
	}()
	for {
		select {
		case <-ticker.C:
			sess := td.GetSessions()
			res := td.GetResources()
			err := snapshot(td.snapshotPath, sess, res)
			if err != nil {
				td.logger.Log(1, "Unable to snapshot: %s", err.Error())
			}
		case <-td.quitSnapChan:
			td.logger.Log(2, "Received quit signal. Exiting snapshot loop...")
			close(td.quitSnapChan) // Signals to caller that we are stopped
			return
		}
	}
}

// Snapshot all the things
func snapshot(path string, sessions map[string]*Session, resources map[string]*Resource) error {
	if err := snapshotSessions(path, sessions); err != nil {
		return fmt.Errorf("unable to snapshot sessions: %s, %s", path, err.Error())

	}
	if err := snapshotResources(path, resources); err != nil {
		return fmt.Errorf("unable to snapshot resources: %s, %s", path, err.Error())
	}
	return nil
}

func snapshotSessions(path string, sessions map[string]*Session) (err error) {
	fn := filepath.Join(path, "sessions.gob")
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	for _, v := range sessions {
		enc.Encode(v)
	}
	return
}

func snapshotResources(path string, resources map[string]*Resource) (err error) {
	fn := filepath.Join(path, "resources.gob")
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := gob.NewEncoder(f)
	for _, v := range resources {
		enc.Encode(v)
	}
	return
}

func loadSessions(path string) (sessions map[string]*Session, err error) {
	sessions = make(map[string]*Session)
	fn := filepath.Join(path, "sessions.gob")
	f, err := os.Open(fn)
	if err != nil {
		return
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	for {
		s := Session{}
		err = dec.Decode(&s)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		sessions[s.Id] = &s
	}
	return
}

func loadResources(path string) (resources map[string]*Resource, err error) {
	resources = make(map[string]*Resource)
	fn := filepath.Join(path, "resources.gob")
	f, err := os.Open(fn)
	if err != nil {
		return
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	for {
		r := Resource{}
		err = dec.Decode(&r)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		resources[r.Name] = &r
	}
	return
}
