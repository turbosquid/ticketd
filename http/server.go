package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/turbosquid/ticketd/ticket"
	"github.com/turbosquid/ticketd/version"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"
)

var timeStarted time.Time = time.Now()

// Ticket response -- adds a "claimed" bool to the base Ticket struct
type TicketResponse struct {
	Claimed bool
	Ticket  ticket.Ticket
}

//
// Server status response. Includes version, uptime, resource usage, etc
type ServerStatusResponse struct {
	Version       string
	Uptime        string
	Started       string
	Uptime_t      time.Duration
	Started_t     time.Time
	NumCpus       int
	GoMaxProcs    int
	NumGoroutines int
	HeapAllocMB   float64
	StackAllocMB  float64
	SysAllocMB    float64
	HeapObjects   uint64
}

func apiErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, ticket.ErrNotFound) {
		code = http.StatusNotFound
	}
	http.Error(w, err.Error(), code)
}

func jsonResp(w http.ResponseWriter, data interface{}, code int) {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	err := enc.Encode(data)
	if err != nil {
		log.Printf("Error encoding outbound data: %s", err.Error())
	}
}

func panicHandler(msg string, w http.ResponseWriter, r *http.Request) {
	http.Error(w, msg, 500)
}

func getSingleQueryParam(url *url.URL, qp string, defaultValue string) (ret string) {
	ret = defaultValue
	if vals, ok := url.Query()[qp]; ok {
		ret = vals[0]
	}
	return
}

func getSingleQueryParamInt(url *url.URL, qp string, defaultValue int) (ret int) {
	ret = defaultValue
	if vals, ok := url.Query()[qp]; ok {
		if n, err := strconv.Atoi(vals[0]); err == nil {
			ret = n
		}
	}
	return
}

// Create a session
func postSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	name := getSingleQueryParam(r.URL, "name", "")
	ttl := getSingleQueryParamInt(r.URL, "ttl", 5000)

	id, err := td.OpenSession(name, r.RemoteAddr, ttl)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, id, 200)
}

// Refresh a session
func putSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	err := td.RefreshSession(id)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "Ok", 200)
}

// Delete (close) a session
func deleteSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	err := td.CloseSession(id)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "Ok", 200)
}

// Get  a session
func getSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	sess, err := td.GetSession(id)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, sess, 200)
}

// Issue a tickwt
func postTickets(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	name := getSingleQueryParam(r.URL, "name", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	if name == "" {
		http.Error(w, "Missing ticket name", http.StatusUnprocessableEntity)
		return
	}
	// Read the request body (ticket data). 1K limit
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
		return
	}
	err = td.IssueTicket(sessid, resource, name, body)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "Ok", 200)
}

// Revoke  a tickwt
func deleteTickets(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	name := getSingleQueryParam(r.URL, "name", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	if name == "" {
		http.Error(w, "Missing ticket name", http.StatusUnprocessableEntity)
		return
	}
	err := td.RevokeTicket(sessid, resource, name)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "Ok", 200)
}

// Claim  a tickwt
func postClaims(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	ok, ticket, err := td.ClaimTicket(sessid, resource)
	if err != nil {
		apiErr(w, err)
		return
	}
	tr := &TicketResponse{}
	tr.Claimed = ok
	if ok {
		tr.Ticket = *ticket
	}
	jsonResp(w, tr, 200)
}

//
// Releae a ticket
func deleteClaims(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	name := getSingleQueryParam(r.URL, "name", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	if name == "" {
		http.Error(w, "Missing ticket name", http.StatusUnprocessableEntity)
		return
	}
	err := td.ReleaseTicket(sessid, resource, name)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "Ok", 200)
}

// Get (check to see if we have)   a tickwt
func getClaims(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	name := getSingleQueryParam(r.URL, "name", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	if name == "" {
		http.Error(w, "Missing ticket name", http.StatusUnprocessableEntity)
		return
	}
	ok, err := td.HasTicket(sessid, resource, name)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, ok, 200)
}

func postLocks(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	ok, err := td.Lock(sessid, resource)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, ok, 200)
}

func deleteLocks(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resource := params.ByName("resource")
	sessid := getSingleQueryParam(r.URL, "sessid", "")
	if sessid == "" {
		http.Error(w, "Missing session id", http.StatusUnprocessableEntity)
		return
	}
	err := td.Unlock(sessid, resource)
	if err != nil {
		apiErr(w, err)
		return
	}
	jsonResp(w, "ok", 200)
}

func getDumpSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	sessions := td.GetSessions()
	jsonResp(w, sessions, 200)
}

func getDumpResources(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resourceName := params.ByName("resource")
	resources := td.GetResources()
	if resourceName != "" {
		r := resources[resourceName]
		if r != nil {
			ret := make(map[string]*ticket.Resource)
			ret[resourceName] = r
			jsonResp(w, ret, 200)
		} else {
			apiErr(w, ticket.ErrNotFound)
		}
		return
	}
	jsonResp(w, resources, 200)
}

func getStatus(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	m := runtime.MemStats{}
	runtime.ReadMemStats(&m)
	resp := ServerStatusResponse{
		Version:       version.VERSION,
		Uptime_t:      time.Now().Sub(timeStarted),
		Started_t:     timeStarted,
		NumCpus:       runtime.NumCPU(),
		GoMaxProcs:    runtime.GOMAXPROCS(-1),
		NumGoroutines: runtime.NumGoroutine(),
		HeapAllocMB:   float64(m.HeapAlloc) / 1048576.0,
		SysAllocMB:    float64(m.Sys) / 1048576.0,
		StackAllocMB:  float64(m.StackInuse) / 1048576.0,
		HeapObjects:   m.HeapObjects,
	}
	resp.Uptime = fmtDuration(resp.Uptime_t)
	// Format start and uptime
	resp.Started = resp.Started_t.Format(time.RFC3339)
	jsonResp(w, resp, 200)
}

//
// Start ticketd api server
func StartServer(listenOn string, td *ticket.TicketD) (svr *http.Server) {
	log.Printf("Starting ticked API server on: %s", listenOn)
	router := httprouter.New()
	svr = &http.Server{
		Addr:    listenOn,
		Handler: router,
	}
	router.POST("/api/v1/sessions", middleWare(td, postSessions))
	router.PUT("/api/v1/sessions/:id", middleWare(td, putSessions))
	router.DELETE("/api/v1/sessions/:id", middleWare(td, deleteSessions))
	router.GET("/api/v1/sessions/:id", middleWare(td, getSessions))
	router.POST("/api/v1/tickets/:resource", middleWare(td, postTickets))
	router.DELETE("/api/v1/tickets/:resource", middleWare(td, deleteTickets))
	router.POST("/api/v1/claims/:resource", middleWare(td, postClaims))
	router.DELETE("/api/v1/claims/:resource", middleWare(td, deleteClaims))
	router.GET("/api/v1/claims/:resource", middleWare(td, getClaims))
	router.POST("/api/v1/locks/:resource", middleWare(td, postLocks))
	router.DELETE("/api/v1/locks/:resource", middleWare(td, deleteLocks))
	router.GET("/api/v1/dump/sessions", middleWare(td, getDumpSessions))
	router.GET("/api/v1/dump/resources", middleWare(td, getDumpResources))
	router.GET("/api/v1/dump/resources/:resource", middleWare(td, getDumpResources))
	router.GET("/api/v1/status", middleWare(td, getStatus))
	go func() {
		if err := svr.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("Unable to start http server on %s -> %s", listenOn, err.Error())
		}
		log.Printf("Stopped ticketd API server by request.")
	}()
	return
}

func middleWare(td *ticket.TicketD, handler func(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params)) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, params httprouter.Params) {
		defer func() {
			if r := recover(); r != nil {
				msg := fmt.Sprintf("%#v", r)
				switch v := r.(type) {
				case string:
					msg = v
				case error:
					msg = v.Error()
				}
				log.Printf("PANIC in http  hander: %s", msg)
				log.Printf("Stack trace:\n%s", debug.Stack())
				panicHandler(msg, w, req)
			}
		}()
		handler(td, w, req, params)
	}
}

func fmtDuration(d time.Duration) string {
	d = d.Round(time.Second)
	days := d / (time.Hour * 24)
	d -= days * time.Hour * 24
	hours := d / time.Hour
	d -= hours * time.Hour
	minutes := d / time.Minute
	d -= minutes * time.Minute
	seconds := d / time.Second
	return fmt.Sprintf("%d days, %02d:%02d:%02d", days, hours, minutes, seconds)
}
