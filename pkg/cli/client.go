// Package cli provides an HA-aware HTTP client for the okube CLI.
// It connects to any manager endpoint, automatically follows leader
// redirects (HTTP 307), and retries across multiple endpoints so that
// the CLI keeps working during leader failover.
package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const maxRedirects = 3

// Client is a high-availability HTTP client that speaks to the manager API.
// It accepts multiple manager endpoints and transparently handles leader
// redirects and failover.
type Client struct {
	Endpoints  []string
	HTTPClient *http.Client
}

// NewClient creates a Client with the supplied manager endpoints.
func NewClient(endpoints []string) *Client {
	return &Client{
		Endpoints: endpoints,
		HTTPClient: &http.Client{
			Timeout: 10 * time.Second,
			// Do NOT follow redirects automatically – we handle 307 ourselves
			// so we can re-send the request body.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

func normalizeEndpoint(ep string) string {
	ep = strings.TrimRight(ep, "/")
	if !strings.HasPrefix(ep, "http://") && !strings.HasPrefix(ep, "https://") {
		ep = "http://" + ep
	}
	return ep
}

// Do executes an HTTP request against the manager cluster. It tries each
// endpoint in order and follows HTTP 307 redirects (up to maxRedirects)
// to reach the current leader.
func (c *Client) Do(method, path string, body interface{}) (*http.Response, error) {
	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
	}

	var lastErr error
	for _, ep := range c.Endpoints {
		url := normalizeEndpoint(ep) + path
		resp, err := c.doWithRedirects(method, url, bodyBytes, maxRedirects)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", ep, err)
			continue
		}
		return resp, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all manager endpoints failed; last error: %w", lastErr)
	}
	return nil, fmt.Errorf("no manager endpoints configured")
}

func (c *Client) doWithRedirects(method, url string, body []byte, remaining int) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	// Follow leader redirects.
	if resp.StatusCode == http.StatusTemporaryRedirect && remaining > 0 {
		loc := resp.Header.Get("Location")
		if loc != "" {
			_ = resp.Body.Close()
			return c.doWithRedirects(method, loc, body, remaining-1)
		}
	}

	return resp, nil
}

// ReadJSON reads and decodes a JSON response body into dst, closing the body.
func ReadJSON(resp *http.Response, dst interface{}) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(dst)
}

// ReadBody reads the full response body as a string, closing the body.
func ReadBody(resp *http.Response) (string, error) {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return string(b), err
}
