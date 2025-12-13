package transport

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

func TestHTTPClientHealthCheck(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("Expected /health, got %s", r.URL.Path)
		}

		resp := protocol.HealthResponse{
			Status:  "OK",
			Address: "localhost:8081",
			Role:    "SLAVE",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Second)

	// Extract just the host:port from the server URL
	addr := server.Listener.Addr().String()

	health, err := client.HealthCheck(addr)
	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}

	if health.Status != "OK" {
		t.Errorf("Expected status OK, got %s", health.Status)
	}

	if health.Role != "SLAVE" {
		t.Errorf("Expected role SLAVE, got %s", health.Role)
	}
}

func TestHTTPClientHealthCheckFails(t *testing.T) {
	client := NewHTTPClient(1 * time.Second)

	// Try to connect to a non-existent server
	_, err := client.HealthCheck("localhost:59999")
	if err == nil {
		t.Error("Expected error for non-existent server")
	}
}

func TestHTTPClientGetRole(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.RoleResponse{
			Role:    "MASTER",
			Address: "localhost:8080",
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Second)
	addr := server.Listener.Addr().String()

	role, err := client.GetRole(addr)
	if err != nil {
		t.Fatalf("GetRole failed: %v", err)
	}

	if role.Role != "MASTER" {
		t.Errorf("Expected MASTER, got %s", role.Role)
	}
}

func TestHTTPClientPrepare(t *testing.T) {
	var receivedTxID string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req protocol.PrepareRequest
		json.NewDecoder(r.Body).Decode(&req)
		receivedTxID = req.TransactionID

		resp := protocol.PrepareResponse{
			Status: protocol.StatusReady,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Second)
	addr := server.Listener.Addr().String()

	req := &protocol.PrepareRequest{
		TransactionID: "test-tx-123",
		Payload:       map[string]string{"key": "value"},
	}

	resp, err := client.Prepare(addr, req)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}

	if receivedTxID != "test-tx-123" {
		t.Errorf("Expected transaction ID test-tx-123, got %s", receivedTxID)
	}

	if resp.Status != protocol.StatusReady {
		t.Errorf("Expected READY, got %s", resp.Status)
	}
}

func TestHTTPClientCommit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.CommitResponse{
			Success: true,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Second)
	addr := server.Listener.Addr().String()

	req := &protocol.CommitRequest{
		TransactionID: "test-tx-123",
	}

	resp, err := client.Commit(addr, req)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success")
	}
}

func TestHTTPClientAbort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.AbortResponse{
			Success: true,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5 * time.Second)
	addr := server.Listener.Addr().String()

	req := &protocol.AbortRequest{
		TransactionID: "test-tx-123",
	}

	resp, err := client.Abort(addr, req)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	if !resp.Success {
		t.Error("Expected success")
	}
}

func TestHTTPClientPrepareRetriesOnServerError(t *testing.T) {
	var attempts int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		resp := protocol.PrepareResponse{
			Status: protocol.StatusReady,
		}
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	client := NewHTTPClient(5*time.Second).WithRetry(1, 5*time.Millisecond)
	addr := server.Listener.Addr().String()

	req := &protocol.PrepareRequest{
		TransactionID: "retry-tx-123",
		Payload:       map[string]string{"key": "value"},
	}

	resp, err := client.Prepare(addr, req)
	if err != nil {
		t.Fatalf("Prepare with retry failed: %v", err)
	}

	if resp.Status != protocol.StatusReady {
		t.Errorf("Expected READY after retry, got %s", resp.Status)
	}

	if attempts != 2 {
		t.Fatalf("Expected 2 attempts (1 retry), got %d", attempts)
	}
}
