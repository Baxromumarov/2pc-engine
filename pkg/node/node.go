package node

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"sync"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// Node represents a single node in the distributed system
type Node struct {
	Addr    string            // address of the node (e.g., "localhost:8081")
	Role    protocol.NodeRole // MASTER or SLAVE
	IsAlive bool              // health status
	TxState protocol.TxState  // current transaction state

	// Transaction management
	pendingTx   map[string]*sql.Tx     // map of transaction_id -> pending transaction
	pendingData map[string]any // simulated data storage for transactions
	mu          sync.RWMutex

	// Database connection (optional, for real DB integration)
	db *sql.DB
}

// NewNode creates a new node instance
func NewNode(addr string, role protocol.NodeRole) *Node {
	return &Node{
		Addr:        addr,
		Role:        role,
		IsAlive:     true,
		TxState:     protocol.StateInit,
		pendingTx:   make(map[string]*sql.Tx),
		pendingData: make(map[string]any),
	}
}

// NewNodeWithDB creates a new node with database connection
func NewNodeWithDB(addr string, role protocol.NodeRole, db *sql.DB) *Node {
	n := NewNode(addr, role)
	n.db = db
	return n
}

// SetAlive updates the node's alive status
func (n *Node) SetAlive(alive bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsAlive = alive
}

// GetAlive returns the node's alive status
func (n *Node) GetAlive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsAlive
}

// SetRole updates the node's role
func (n *Node) SetRole(role protocol.NodeRole) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Role = role
}

// GetRole returns the node's current role
func (n *Node) GetRole() protocol.NodeRole {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.Role
}

// Prepare handles the prepare phase of 2PC
// Returns true if ready to commit, false otherwise
func (n *Node) Prepare(txID string, payload any) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we already have a pending transaction with this ID
	if _, exists := n.pendingData[txID]; exists {
		return false, errors.New("transaction already in progress")
	}

	// If we have a real database connection, start a transaction
	if n.db != nil {
		tx, err := n.db.BeginTx(context.Background(), nil)
		if err != nil {
			log.Printf("[Node %s] Failed to begin transaction: %v", n.Addr, err)
			return false, err
		}

		// Simulate executing some work (in real system, this would be actual SQL)
		// For now, we just store the transaction
		n.pendingTx[txID] = tx
	}

	// Store the payload for simulated transaction
	n.pendingData[txID] = payload
	n.TxState = protocol.StateReady

	log.Printf("[Node %s] Prepared transaction %s", n.Addr, txID)
	return true, nil
}

// Commit commits the prepared transaction
func (n *Node) Commit(txID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we have a real transaction, commit it
	if tx, exists := n.pendingTx[txID]; exists {
		if err := tx.Commit(); err != nil {
			log.Printf("[Node %s] Failed to commit transaction %s: %v", n.Addr, txID, err)
			return err
		}
		delete(n.pendingTx, txID)
	}

	// Clean up simulated data
	delete(n.pendingData, txID)
	n.TxState = protocol.StateCommit

	log.Printf("[Node %s] Committed transaction %s", n.Addr, txID)
	return nil
}

// Abort rolls back the prepared transaction
func (n *Node) Abort(txID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we have a real transaction, rollback
	if tx, exists := n.pendingTx[txID]; exists {
		if err := tx.Rollback(); err != nil {
			log.Printf("[Node %s] Failed to rollback transaction %s: %v", n.Addr, txID, err)
			return err
		}
		delete(n.pendingTx, txID)
	}

	// Clean up simulated data
	delete(n.pendingData, txID)
	n.TxState = protocol.StateAbort

	log.Printf("[Node %s] Aborted transaction %s", n.Addr, txID)
	return nil
}

// HasPendingTransaction checks if a transaction is pending
func (n *Node) HasPendingTransaction(txID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, exists := n.pendingData[txID]
	return exists
}

// GetPendingTransactions returns all pending transaction IDs
func (n *Node) GetPendingTransactions() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	txIDs := make([]string, 0, len(n.pendingData))
	for txID := range n.pendingData {
		txIDs = append(txIDs, txID)
	}
	return txIDs
}
