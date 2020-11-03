package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/turbosquid/ticketd/ticket"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const apiPath = "/api/v1"

type HttpError struct {
	Code    int
	Message string
}

func (err *HttpError) Error() string {
	return err.Message
}

func NewHttpError(code int, msg string) (err *HttpError) {
	return &HttpError{code, msg}
}

func HttpErrorCode(err error) (code int) {
	if err == nil {
		return
	}
	if herr, ok := err.(*HttpError); ok {
		code = herr.Code
	}
	return
}

type Client struct {
	baseUrl string
	http.Client
}

type Session struct {
	c             *Client
	Id            string
	heartBeatChan chan interface{}
	heartBeatWg   sync.WaitGroup
}

func NewClient(url string, timeout time.Duration) (c *Client) {
	c = &Client{url, http.Client{Timeout: timeout}}
	return
}

func (c *Client) urlStr(path string) string {
	return fmt.Sprintf("%s%s%s", c.baseUrl, apiPath, path)
}

func (c *Client) callBytes(verb, path string, in []byte, objOut interface{}) (err error) {
	var request *http.Request
	if in != nil {
		request, err = http.NewRequest(verb, c.urlStr(path), bytes.NewBuffer(in))
	} else {
		request, err = http.NewRequest(verb, c.urlStr(path), nil)
	}
	if err != nil {
		return
	}
	request.Header.Set("Content-type", "application/json")
	resp, err := c.Do(request)
	if err != nil {
		return
	}
	code := resp.StatusCode
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if code >= 300 {
		err = NewHttpError(code, fmt.Sprintf("HTTP %d = %s", code, string(body)))
	} else {
		// fmt.Printf("%d\n%s", code, string(body))
		err = json.Unmarshal(body, objOut)
	}
	return
}

func (c *Client) call(verb, path string, obj interface{}, objOut interface{}) (err error) {
	var requestBody []byte
	if obj != nil {
		requestBody, err = json.Marshal(obj)
		if err != nil {
			return
		}
	}
	err = c.callBytes(verb, path, requestBody, objOut)
	return
}

//
// Opena new session
func (c *Client) OpenSession(name string, ttlMs int) (session *Session, err error) {
	id := ""
	name = url.QueryEscape(name)
	err = c.call("POST", fmt.Sprintf("/sessions?name=%s&ttl=%d", name, ttlMs), nil, &id)
	if err != nil {
		return
	}
	session = &Session{c, id, nil, sync.WaitGroup{}}
	return
}

//
// Close this session
func (s *Session) Close() (err error) {
	s.CancelHeartBeat()
	errMsg := ""
	err = s.c.call("DELETE", fmt.Sprintf("/sessions/%s", s.Id), nil, &errMsg)
	if err != nil {
		return
	}
	return
}

//
// Refresh this session at server
func (s *Session) Refresh() (err error) {
	errMsg := ""
	err = s.c.call("PUT", fmt.Sprintf("/sessions/%s", s.Id), nil, &errMsg)
	if err != nil {
		return
	}
	return
}

//
// Get a copy of this session
func (s *Session) Get() (sess *ticket.Session, err error) {
	sess = &ticket.Session{}
	err = s.c.call("GET", fmt.Sprintf("/sessions/%s", s.Id), nil, sess)
	if err != nil {
		return
	}
	return
}

//
// Run background "heartbeat" session refresh. Keeps session alive until
// a) The session is closed, or
// b) an http error occurs, or
// c) any other error occurs, unless we specify to ignore these. The idea is to optionally ignore transient connection errorsa
//
// interval -- interval between refreshes
// timeout -- http timeout for call
// ignoreNonHttpErrors -- keep trying on non-http errors
// notify  -- function to call on backgrund proc exit. Will pass error or nil
func (s *Session) RunHeartbeat(interval time.Duration, timeout time.Duration, ignoreNonHttpErrors bool, notify func(err error)) {
	s.heartBeatChan = make(chan interface{})
	// Make a copy of the session and change the timeout
	sessCopy := *s
	sessCopy.c.Timeout = timeout
	s.heartBeatWg.Add(1)
	go func() {
		defer s.heartBeatWg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.heartBeatChan:
				go notify(nil)
				return
			case <-ticker.C:
				err := sessCopy.Refresh()
				code := HttpErrorCode(err)
				if err != nil && (!ignoreNonHttpErrors || code != 0) {
					go notify(err)
					return
				}
			}
		}
	}()
}

//
// Cancel heartbeat proc -- if running, else a noop
func (s *Session) CancelHeartBeat() {
	if s.heartBeatChan != nil {
		close(s.heartBeatChan)
		s.heartBeatWg.Wait()
		s.heartBeatChan = nil
	}
}

//
// Issue and revoke tickets
func (s *Session) IssueTicket(resource, name string, data []byte) (err error) {
	errMsg := ""
	name = url.QueryEscape(name)
	err = s.c.callBytes("POST", fmt.Sprintf("/tickets/%s?name=%s&sessid=%s", resource, name, s.Id), data, &errMsg)
	return
}

func (s *Session) RevokeTicket(resource, name string) (err error) {
	errMsg := ""
	name = url.QueryEscape(name)
	err = s.c.call("DELETE", fmt.Sprintf("/tickets/%s?name=%s&sessid=%s", resource, name, s.Id), nil, &errMsg)
	return
}

//
// Claim and release tickets
func (s *Session) ClaimTicket(resource string) (ok bool, ticket *ticket.Ticket, err error) {
	resp := &TicketResponse{}
	err = s.c.call("POST", fmt.Sprintf("/claims/%s?sessid=%s", resource, s.Id), nil, resp)
	if err != nil {
		return
	}
	if !resp.Claimed {
		return false, nil, nil
	}
	ok = true
	ticket = &(resp.Ticket)
	return
}

func (s *Session) ReleaseTicket(resource, name string) (err error) {
	errMsg := ""
	name = url.QueryEscape(name)
	err = s.c.call("DELETE", fmt.Sprintf("/claims/%s?name=%s&sessid=%s", resource, name, s.Id), nil, &errMsg)
	return
}

func (s *Session) HasTicket(resource, name string) (ok bool, err error) {
	name = url.QueryEscape(name)
	err = s.c.call("GET", fmt.Sprintf("/claims/%s?name=%s&sessid=%s", resource, name, s.Id), nil, &ok)
	return
}

func (s *Session) Lock(resource string) (ok bool, err error) {
	err = s.c.call("POST", fmt.Sprintf("/locks/%s?sessid=%s", resource, s.Id), nil, &ok)
	return
}

func (s *Session) Unlock(resource string) (err error) {
	errMsg := ""
	err = s.c.call("DELETE", fmt.Sprintf("/locks/%s?sessid=%s", resource, s.Id), nil, &errMsg)
	return
}

func (c *Client) GetSessions() (sessions map[string]*ticket.Session, err error) {
	err = c.call("GET", "/dump/sessions", nil, &sessions)
	return
}

func (c *Client) GetResources() (resources map[string]*ticket.Resource, err error) {
	err = c.call("GET", "/dump/resources", nil, &resources)
	return
}
