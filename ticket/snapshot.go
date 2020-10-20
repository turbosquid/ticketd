package ticket

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func (td *TicketD) LoadSnapshot() (sessions map[string]*Session, resources map[string]*Resource, err error) {

	sessions, err = loadSessions(td.snapshotPath)
	if err != nil {
		return
	}
	resources, err = loadResources(td.snapshotPath)
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
				return nil, nil, fmt.Errorf("Unable to find resource %s", ticket.ResourceName)
			}
			// Replace ticket pointer here to pointer from resources
			sess.Tickets[i] = res.Tickets[ticket.Name]
			if sess.Tickets[i] == nil {
				return nil, nil, fmt.Errorf("Ticket %s does not exist for resource %s", ticket.Name, res.Name)
			}
			// Updateticket claimant pointer to THIS session pointer
			sess.Tickets[i].Claimant = sess
		}
		for i, ticket := range sess.Issuances {
			// Be sure the things we THINK exist exist in resources
			res := resources[ticket.ResourceName]
			if res == nil {
				return nil, nil, fmt.Errorf("Unable to find resource %s", ticket.ResourceName)
			}
			// Replace ticket pointer here to pointer from resources
			sess.Issuances[i] = res.Tickets[ticket.Name]
			if sess.Issuances[i] == nil {
				return nil, nil, fmt.Errorf("Ticket %s does not exist for resource %s", ticket.Name, res.Name)
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

//
// Optional snapshot loop
func (td *TicketD) Snapshot() {
	ticker := time.NewTicker(time.Duration(td.snapshotInterval) * time.Millisecond)
	log.Printf("Snapshot loop starting...")
	os.MkdirAll(td.snapshotPath, 0755)
	for {
		select {
		case _ = <-ticker.C:
			sess := td.GetSessions()
			err := snapshotSessions(td.snapshotPath, sess)
			if err != nil {
				log.Printf("Unable to snapshot sessions: %s, %s", td.snapshotPath, err.Error())
			}
			res := td.GetResources()
			err = snapshotResources(td.snapshotPath, res)
			if err != nil {
				log.Printf("Unable to snapshot resources: %s, %s", td.snapshotPath, err.Error())
			}
		case _ = <-td.quitChan:
			log.Printf("Received quit signal. Exiting snapshot loop...")
			return
		}
	}
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
