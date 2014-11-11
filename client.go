package gopenstack

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type Client struct {
	client     *http.Client
	xAuthToken string
	endpoint   string
}

// newRaClient returns a new apiClient
func NewClient(keyring *Keyring, region, iType string) (c *Client, err error) {
	c = new(Client)
	c.client = &http.Client{}
	c.xAuthToken = keyring.XAuthHeaderToken
	c.endpoint, err = keyring.GetEndpointUrl(iType, region)
	return
}

// cResponse represent a openstack API response
type cResponse struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
	BodyReader io.ReadCloser
}

// handleCommon return error on unexpected HTTP code
func (r *cResponse) HandleErr(err error, expectedHttpCode []int) error {
	if err != nil {
		return err
	}
	for _, code := range expectedHttpCode {
		if r.StatusCode == code {
			return nil
		}
	}
	return errors.New(fmt.Sprintf("%d - %s", r.StatusCode, r.Status))
}

type CallOptions struct {
	Method             string
	Ressource          string
	Headers            map[string]string
	Payload            io.Reader
	ReturnBodyAsReader bool
}

func (c *Client) Call(options *CallOptions) (response *cResponse, err error) {
	var req *http.Request
	response = new(cResponse)
	if options.Ressource[0] == 47 {
		options.Ressource = options.Ressource[1:]
	}
	query := fmt.Sprintf("%s/%s", c.endpoint, options.Ressource)

	req, err = http.NewRequest(options.Method, query, options.Payload)
	if err != nil {
		return response, err
	}

	//req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Auth-Token", c.xAuthToken)
	req.Header.Add("User-Agent", "gopenstack (https://github.com/Toorop/gopenstack)")

	// Extra headers
	for k, v := range options.Headers {
		req.Header.Add(k, v)
	}

	resp, err := c.client.Do(req)

	if err != nil {
		return
	}
	if options.ReturnBodyAsReader {
		response.BodyReader = resp.Body
	} else {
		defer resp.Body.Close()
		response.Body, err = ioutil.ReadAll(resp.Body)
	}
	response.StatusCode = resp.StatusCode
	response.Status = resp.Status
	response.Headers = resp.Header
	return
}
