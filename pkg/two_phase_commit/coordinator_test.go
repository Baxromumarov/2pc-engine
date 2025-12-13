package twophasecommit

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/cluster"
	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// TestSuccessful2PC tests the happy path where all nodes prepare and commit successfully
func TestSuccessful2PC(t *testing.T) {
	// Create mock nodes
	node1 := createMockNode(t, true, true) // prepare success, commit success
	node2 := createMockNode(t, true, true)
	defer node1.Close()
	defer node2.Close()

	// Create cluster with mock nodes
	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	// Add slave nodes pointing to mock servers
	slave1 := node.NewNode(node1.Listener.Addr().String(), protocol.RoleSlave)
	slave2 := node.NewNode(node2.Listener.Addr().String(), protocol.RoleSlave)
	slave1.SetAlive(true)
	slave2.SetAlive(true)
	c.AddNode(slave1)
	c.AddNode(slave2)

	// Create coordinator and execute (nil localNode = master doesn't participate)
	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success, got failure: %s", resp.Error)
	}

	if resp.TransactionID == "" {
		t.Error("Expected transaction ID to be set")
	}
}

// TestPrepareFails tests when one node fails prepare
func TestPrepareFails(t *testing.T) {
	// Create mock nodes - one fails prepare
	node1 := createMockNode(t, true, true)  // prepare success
	node2 := createMockNode(t, false, true) // prepare fails
	defer node1.Close()
	defer node2.Close()

	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	slave1 := node.NewNode(node1.Listener.Addr().String(), protocol.RoleSlave)
	slave2 := node.NewNode(node2.Listener.Addr().String(), protocol.RoleSlave)
	slave1.SetAlive(true)
	slave2.SetAlive(true)
	c.AddNode(slave1)
	c.AddNode(slave2)

	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.Success {
		t.Error("Expected failure when one node fails prepare")
	}
}

// TestNoParticipants tests when there are no participants available
func TestNoParticipants(t *testing.T) {
	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.Success {
		t.Error("Expected failure when no participants")
	}

	if resp.Error != "No participants available" {
		t.Errorf("Expected 'No participants available' error, got: %s", resp.Error)
	}
}

// createMockNode creates a mock HTTP server that simulates a node
func createMockNode(t *testing.T, prepareSuccess, commitSuccess bool) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/prepare", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.PrepareResponse{}
		if prepareSuccess {
			resp.Status = protocol.StatusReady
			w.WriteHeader(http.StatusOK)
		} else {
			resp.Status = protocol.StatusAbort
			resp.Error = "Prepare failed"
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/commit", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.CommitResponse{Success: commitSuccess}
		if !commitSuccess {
			resp.Error = "Commit failed"
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/abort", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.AbortResponse{Success: true}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.HealthResponse{Status: "OK", Role: "SLAVE"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	return httptest.NewServer(mux)
}
