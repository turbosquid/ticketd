package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"github.com/turbosquid/ticketd/ticket"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"
)

func HttpError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(code)
	w.Write([]byte(msg))
}

func ApiErr(w http.ResponseWriter, err error) {
	code := 500
	if errors.Is(err, ticket.ErrNotFound) {
		code = 404
	}
	Json(w, err.Error(), code)
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
	HttpError(w, msg, 500)
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
