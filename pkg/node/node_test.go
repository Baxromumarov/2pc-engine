package node

import (
	"testing"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

func TestNewNode(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	if n.Addr != "localhost:8081" {
		t.Errorf("Expected addr localhost:8081, got %s", n.Addr)
	}

	if n.GetRole() != protocol.RoleSlave {
		t.Errorf("Expected role SLAVE, got %s", n.GetRole())
	}

	if !n.GetAlive() {
		t.Error("Expected new node to be alive")
	}
}

func TestNodeSetAlive(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	n.SetAlive(false)
	if n.GetAlive() {
		t.Error("Expected node to be not alive")
	}

	n.SetAlive(true)
	if !n.GetAlive() {
		t.Error("Expected node to be alive")
	}
}

func TestNodeSetRole(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	n.SetRole(protocol.RoleMaster)
	if n.GetRole() != protocol.RoleMaster {
		t.Error("Expected role to be MASTER")
	}

	n.SetRole(protocol.RoleSlave)
	if n.GetRole() != protocol.RoleSlave {
		t.Error("Expected role to be SLAVE")
	}
}

func TestNodePrepareCommit(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	// Prepare a transaction
	txID := "tx-123"
	payload := map[string]string{"key": "value"}

	ready, err := n.Prepare(txID, payload)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if !ready {
		t.Error("Expected prepare to succeed")
	}

	// Check transaction is pending
	if !n.HasPendingTransaction(txID) {
		t.Error("Expected transaction to be pending")
	}

	// Commit the transaction
	err = n.Commit(txID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check transaction is no longer pending
	if n.HasPendingTransaction(txID) {
		t.Error("Expected transaction to be committed and removed")
	}
}

func TestNodePrepareAbort(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	txID := "tx-456"
	payload := map[string]string{"key": "value"}

	ready, err := n.Prepare(txID, payload)
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if !ready {
		t.Error("Expected prepare to succeed")
	}

	// Abort the transaction
	err = n.Abort(txID)
	if err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	// Check transaction is no longer pending
	if n.HasPendingTransaction(txID) {
		t.Error("Expected transaction to be aborted and removed")
	}
}

func TestNodeDuplicatePrepare(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	txID := "tx-789"
	payload := map[string]string{"key": "value"}

	// First prepare should succeed
	ready, err := n.Prepare(txID, payload)
	if err != nil || !ready {
		t.Fatal("First prepare should succeed")
	}

	// Second prepare with same ID should fail
	ready, err = n.Prepare(txID, payload)
	if ready {
		t.Error("Duplicate prepare should fail")
	}
	if err == nil {
		t.Error("Expected error for duplicate prepare")
	}
}

func TestNodeGetPendingTransactions(t *testing.T) {
	n := NewNode("localhost:8081", protocol.RoleSlave)

	// Prepare multiple transactions
	n.Prepare("tx-1", nil)
	n.Prepare("tx-2", nil)
	n.Prepare("tx-3", nil)

	pending := n.GetPendingTransactions()
	if len(pending) != 3 {
		t.Errorf("Expected 3 pending transactions, got %d", len(pending))
	}

	// Commit one
	n.Commit("tx-1")

	pending = n.GetPendingTransactions()
	if len(pending) != 2 {
		t.Errorf("Expected 2 pending transactions after commit, got %d", len(pending))
	}
}
