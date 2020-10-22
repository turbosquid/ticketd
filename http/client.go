package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/turbosquid/ticketd/ticket"
	"io/ioutil"
	"net/http"
	"time"
)

const apiPath = "/api/v1"

type Client struct {
	baseUrl string
	http.Client
}

type Session struct {
	c  *Client
	Id string
}

func NewClient(url string, timeout time.Duration) (c *Client) {
	c = &Client{url, http.Client{Timeout: timeout}}
	return
}

func (c *Client) urlStr(path string) string {
	return fmt.Sprintf("%s%s%s", c.baseUrl, apiPath, path)
}

func (c *Client) call(verb, path string, obj interface{}, objOut interface{}) (code int, err error) {
	var requestBody []byte
	if obj != nil {
		requestBody, err = json.Marshal(obj)
		if err != nil {
			return
		}
	}
	var request *http.Request
	if requestBody != nil {
		request, err = http.NewRequest(verb, c.urlStr(path), bytes.NewBuffer(requestBody))
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
	code = resp.StatusCode
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	// fmt.Printf("%d\n%s", code, string(body))
	err = json.Unmarshal(body, objOut)
	return
}

func (c *Client) OpenSession(name string, ttlMs int) (session *Session, err error) {
	id := ""
	code, err := c.call("POST", fmt.Sprintf("/sessions?name=%s&ttl=%d", name, ttlMs), nil, &id)
	if err != nil {
		return
	}
	if code >= 300 {
		return nil, fmt.Errorf("Non http success code: %d", code)
	}
	session = &Session{c, id}
	return
}

func (s *Session) Close() (err error) {
	errMsg := ""
	code, err := s.c.call("DELETE", fmt.Sprintf("/sessions/%s", s.Id), nil, &errMsg)
	if err != nil {
		return
	}
	if code >= 300 {
		return fmt.Errorf("Non http success code: %d (%s)", code, errMsg)
	}
	return
}

func (s *Session) Refresh() (err error) {
	errMsg := ""
	code, err := s.c.call("PUT", fmt.Sprintf("/sessions/%s", s.Id), nil, &errMsg)
	if err != nil {
		return
	}
	if code >= 300 {
		return fmt.Errorf("Non http success code: %d (%s)", code, errMsg)
	}
	return
}

func (s *Session) Get() (sess *ticket.Session, err error) {
	sess = &ticket.Session{}
	code, err := s.c.call("GET", fmt.Sprintf("/sessions/%s", s.Id), nil, sess)
	if err != nil {
		return
	}
	if code >= 300 {
		return nil, fmt.Errorf("Non http success code: %d", code)
	}
	return
}
