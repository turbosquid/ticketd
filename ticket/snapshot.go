package ticket

import (
	"encoding/gob"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"
)

func (td *TicketD) LoadSnapshot(path string) (sessions map[string]*Session, resources map[string]*Resource, err error) {

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
		for i, ticket := range sess.Tickets {
			// Replace ticket pointer here to pointer from resources
			sess.Tickets[i] = resources[ticket.ResourceName].Tickets[ticket.Name]
			// Updateticket claimant pointer to THIS session pointer
			sess.Tickets[i].Claimant = sess
		}
		for i, ticket := range sess.Issuances {
			// Replace ticket pointer here to pointer from resources
			sess.Issuances[i] = resources[ticket.ResourceName].Tickets[ticket.Name]
			sess.Issuances[i].Issuer = sess
		}
		sess.refresh()
	}
	return
}

//
// Optional snapshot loop
func (td *TicketD) Snapshot(intervalMs int, path string) {
	ticker := time.NewTicker(time.Duration(td.expireTickTimeMs) * time.Millisecond)
	log.Printf("Snapshot loop starting...")
	os.MkdirAll(path, 0755)
	for {
		select {
		case _ = <-ticker.C:
			sess := td.GetSessions()
			err := snapshotSessions(path, sess)
			if err != nil {
				log.Printf("Unable to snapshot sessions: %s, %s", path, err.Error())
			}
			res := td.GetResources()
			err = snapshotResources(path, res)
			if err != nil {
				log.Printf("Unable to snapshot resources: %s, %s", path, err.Error())
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
		sessions[s.Name] = &s
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
