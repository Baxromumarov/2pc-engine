package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// HTTPClient handles HTTP communication between nodes
type HTTPClient struct {
	client  *http.Client
	timeout time.Duration
	// retry configuration; kept simple to avoid changing public constructors
	maxRetries int
	retryDelay time.Duration
}

// NewHTTPClient creates a new HTTP client with timeout
func NewHTTPClient(timeout time.Duration) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
	}
}

// WithRetry configures retry attempts for transient failures (5xx or transport errors).
// Retries are disabled by default to preserve existing semantics.
func (c *HTTPClient) WithRetry(maxRetries int, retryDelay time.Duration) *HTTPClient {
	if maxRetries < 0 {
		maxRetries = 0
	}
	if retryDelay < 0 {
		retryDelay = 0
	}

	c.maxRetries = maxRetries
	c.retryDelay = retryDelay
	return c
}

// DefaultHTTPClient creates a client with default 5 second timeout
func DefaultHTTPClient() *HTTPClient {
	return NewHTTPClient(5 * time.Second)
}

// HealthCheck checks if a node is alive
func (c *HTTPClient) HealthCheck(addr string) (*protocol.HealthResponse, error) {
	resp, err := c.doWithRetry(func() (*http.Response, error) {
		return c.client.Get(fmt.Sprintf("http://%s/health", addr))
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health check failed with status: %d", resp.StatusCode)
	}

	var health protocol.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		return nil, err
	}

	return &health, nil
}

// GetRole gets the current role of a node
func (c *HTTPClient) GetRole(addr string) (*protocol.RoleResponse, error) {
	resp, err := c.doWithRetry(func() (*http.Response, error) {
		return c.client.Get(fmt.Sprintf("http://%s/role", addr))
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get role failed with status: %d", resp.StatusCode)
	}

	var role protocol.RoleResponse
	if err := json.NewDecoder(resp.Body).Decode(&role); err != nil {
		return nil, err
	}

	return &role, nil
}

// Prepare sends a prepare request to a node
func (c *HTTPClient) Prepare(addr string, req *protocol.PrepareRequest) (*protocol.PrepareResponse, error) {
	resp, err := c.postJSON(addr, "prepare", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return decodePrepareResponse(resp.Body)
}

// Commit sends a commit request to a node
func (c *HTTPClient) Commit(addr string, req *protocol.CommitRequest) (*protocol.CommitResponse, error) {
	resp, err := c.postJSON(addr, "commit", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return decodeCommitResponse(resp.Body)
}

// Abort sends an abort request to a node
func (c *HTTPClient) Abort(addr string, req *protocol.AbortRequest) (*protocol.AbortResponse, error) {
	resp, err := c.postJSON(addr, "abort", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return decodeAbortResponse(resp.Body)
}

// StartTransaction sends a transaction request to the master
func (c *HTTPClient) StartTransaction(masterAddr string, req *protocol.TransactionRequest) (*protocol.TransactionResponse, error) {
	resp, err := c.postJSON(masterAddr, "transaction", req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return decodeTransactionResponse(resp.Body)
}

func (c *HTTPClient) postJSON(addr, path string, payload any) (*http.Response, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return c.doWithRetry(func() (*http.Response, error) {
		return c.client.Post(
			fmt.Sprintf("http://%s/%s", addr, path),
			"application/json",
			bytes.NewReader(body),
		)
	})
}

func (c *HTTPClient) doWithRetry(do func() (*http.Response, error)) (*http.Response, error) {
	attempts := c.maxRetries + 1
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		resp, err := do()
		if err == nil && resp.StatusCode < http.StatusInternalServerError {
			return resp, nil
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("transient status: %d", resp.StatusCode)
			// Ensure we drain/close to avoid leaking connections
			if resp.Body != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}

		if attempt == attempts-1 {
			break
		}

		if c.retryDelay > 0 {
			time.Sleep(c.retryDelay)
		}
	}

	return nil, lastErr
}

func decodePrepareResponse(body io.Reader) (*protocol.PrepareResponse, error) {
	var prepareResp protocol.PrepareResponse
	if err := json.NewDecoder(body).Decode(&prepareResp); err != nil {
		return nil, err
	}
	return &prepareResp, nil
}

func decodeCommitResponse(body io.Reader) (*protocol.CommitResponse, error) {
	var commitResp protocol.CommitResponse
	if err := json.NewDecoder(body).Decode(&commitResp); err != nil {
		return nil, err
	}
	return &commitResp, nil
}

func decodeAbortResponse(body io.Reader) (*protocol.AbortResponse, error) {
	var abortResp protocol.AbortResponse
	if err := json.NewDecoder(body).Decode(&abortResp); err != nil {
		return nil, err
	}
	return &abortResp, nil
}

func decodeTransactionResponse(body io.Reader) (*protocol.TransactionResponse, error) {
	var txResp protocol.TransactionResponse
	if err := json.NewDecoder(body).Decode(&txResp); err != nil {
		return nil, err
	}
	return &txResp, nil
}
