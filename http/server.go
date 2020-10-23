package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/turbosquid/ticketd/ticket"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
)

// Ticket response -- adds a "claimed" bool
type TicketResponse struct {
	Claimed bool
	Ticket  ticket.Ticket
}

func ApiErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	if errors.Is(err, ticket.ErrNotFound) {
		code = http.StatusNotFound
	}
	http.Error(w, err.Error(), code)
}

func Json(w http.ResponseWriter, data interface{}, code int) {
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
		ApiErr(w, err)
		return
	}
	Json(w, id, 200)
}

// Refresh a session
func putSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	err := td.RefreshSession(id)
	if err != nil {
		ApiErr(w, err)
		return
	}
	Json(w, "Ok", 200)
}

// Delete (close) a session
func deleteSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	err := td.CloseSession(id)
	if err != nil {
		ApiErr(w, err)
		return
	}
	Json(w, "Ok", 200)
}

// Get  a session
func getSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	id := params.ByName("id")

	sess, err := td.GetSession(id)
	if err != nil {
		ApiErr(w, err)
		return
	}
	Json(w, sess, 200)
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
		ApiErr(w, err)
		return
	}
	Json(w, "Ok", 200)
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
	err := td.ReleaseTicket(sessid, resource, name)
	if err != nil {
		ApiErr(w, err)
		return
	}
	Json(w, "Ok", 200)
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
		ApiErr(w, err)
		return
	}
	log.Printf("postClaims ticket %#v", ticket)
	tr := &TicketResponse{}
	tr.Claimed = ok
	if ok {
		tr.Ticket = *ticket
	}
	Json(w, tr, 200)
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
		ApiErr(w, err)
		return
	}
	Json(w, "Ok", 200)
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
		ApiErr(w, err)
		return
	}
	Json(w, ok, 200)
}

func getDumpSessions(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	sessions := td.GetSessions()
	Json(w, sessions, 200)
}

func getDumpResources(td *ticket.TicketD, w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	resources := td.GetResources()
	Json(w, resources, 200)
}

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
	router.GET("/api/v1/dump/sessions", middleWare(td, getDumpSessions))
	router.GET("/api/v1/dump/resources", middleWare(td, getDumpResources))
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
