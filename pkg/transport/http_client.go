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

// DefaultHTTPClient creates a client with default 5 second timeout
func DefaultHTTPClient() *HTTPClient {
	return NewHTTPClient(5 * time.Second)
}

// HealthCheck checks if a node is alive
func (c *HTTPClient) HealthCheck(addr string) (*protocol.HealthResponse, error) {
	resp, err := c.client.Get(fmt.Sprintf("http://%s/health", addr))
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
	resp, err := c.client.Get(fmt.Sprintf("http://%s/role", addr))
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
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Post(
		fmt.Sprintf("http://%s/prepare", addr),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var prepareResp protocol.PrepareResponse
	if err := json.Unmarshal(respBody, &prepareResp); err != nil {
		return nil, err
	}

	return &prepareResp, nil
}

// Commit sends a commit request to a node
func (c *HTTPClient) Commit(addr string, req *protocol.CommitRequest) (*protocol.CommitResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Post(
		fmt.Sprintf("http://%s/commit", addr),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var commitResp protocol.CommitResponse
	if err := json.Unmarshal(respBody, &commitResp); err != nil {
		return nil, err
	}

	return &commitResp, nil
}

// Abort sends an abort request to a node
func (c *HTTPClient) Abort(addr string, req *protocol.AbortRequest) (*protocol.AbortResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Post(
		fmt.Sprintf("http://%s/abort", addr),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var abortResp protocol.AbortResponse
	if err := json.Unmarshal(respBody, &abortResp); err != nil {
		return nil, err
	}

	return &abortResp, nil
}

// StartTransaction sends a transaction request to the master
func (c *HTTPClient) StartTransaction(masterAddr string, req *protocol.TransactionRequest) (*protocol.TransactionResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Post(
		fmt.Sprintf("http://%s/transaction", masterAddr),
		"application/json",
		bytes.NewBuffer(body),
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var txResp protocol.TransactionResponse
	if err := json.Unmarshal(respBody, &txResp); err != nil {
		return nil, err
	}

	return &txResp, nil
}
