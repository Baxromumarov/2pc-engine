package twophasecommit

import (
	"log"
	"sync"

	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// Participant represents a node participating in 2PC
type Participant struct {
	node         *node.Node
	transactions map[string]*TransactionState
	mu           sync.RWMutex
}

// TransactionState holds the state of a transaction on a participant
type TransactionState struct {
	ID      string
	State   protocol.TxState
	Payload any
}

// NewParticipant creates a new participant wrapper
func NewParticipant(n *node.Node) *Participant {
	return &Participant{
		node:         n,
		transactions: make(map[string]*TransactionState),
	}
}

// Prepare handles the prepare phase
func (p *Participant) Prepare(txID string, payload any) *protocol.PrepareResponse {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if transaction already exists
	if _, exists := p.transactions[txID]; exists {
		log.Printf("[Participant %s] Transaction %s already exists", p.node.Addr, txID)
		return &protocol.PrepareResponse{
			Status: protocol.StatusAbort,
			Error:  "Transaction already in progress",
		}
	}

	// Try to prepare the transaction on the node
	ready, err := p.node.Prepare(txID, payload)
	if !ready || err != nil {
		errMsg := "Prepare failed"
		if err != nil {
			errMsg = err.Error()
		}
		log.Printf("[Participant %s] Failed to prepare transaction %s: %s", p.node.Addr, txID, errMsg)
		return &protocol.PrepareResponse{
			Status: protocol.StatusAbort,
			Error:  errMsg,
		}
	}

	// Store transaction state
	p.transactions[txID] = &TransactionState{
		ID:      txID,
		State:   protocol.StateReady,
		Payload: payload,
	}

	log.Printf("[Participant %s] Prepared transaction %s", p.node.Addr, txID)
	return &protocol.PrepareResponse{
		Status: protocol.StatusReady,
	}
}

// Commit handles the commit phase
func (p *Participant) Commit(txID string) *protocol.CommitResponse {
	p.mu.Lock()
	defer p.mu.Unlock()

	txState, exists := p.transactions[txID]
	if !exists {
		log.Printf("[Participant %s] Transaction %s not found for commit", p.node.Addr, txID)
		return &protocol.CommitResponse{
			Success: false,
			Error:   "Transaction not found",
		}
	}

	if txState.State != protocol.StateReady {
		log.Printf("[Participant %s] Transaction %s not in READY state", p.node.Addr, txID)
		return &protocol.CommitResponse{
			Success: false,
			Error:   "Transaction not in READY state",
		}
	}

	// Commit on the node
	if err := p.node.Commit(txID); err != nil {
		log.Printf("[Participant %s] Failed to commit transaction %s: %v", p.node.Addr, txID, err)
		return &protocol.CommitResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	// Update and cleanup transaction state
	txState.State = protocol.StateCommit
	delete(p.transactions, txID)

	log.Printf("[Participant %s] Committed transaction %s", p.node.Addr, txID)
	return &protocol.CommitResponse{
		Success: true,
	}
}

// Abort handles the abort phase
func (p *Participant) Abort(txID string) *protocol.AbortResponse {
	p.mu.Lock()
	defer p.mu.Unlock()

	txState, exists := p.transactions[txID]
	if !exists {
		// Transaction might not exist if prepare failed
		log.Printf("[Participant %s] Transaction %s not found for abort (may not have been prepared)", p.node.Addr, txID)
		return &protocol.AbortResponse{
			Success: true,
		}
	}

	// Abort on the node
	if err := p.node.Abort(txID); err != nil {
		log.Printf("[Participant %s] Failed to abort transaction %s: %v", p.node.Addr, txID, err)
		return &protocol.AbortResponse{
			Success: false,
			Error:   err.Error(),
		}
	}

	// Update and cleanup transaction state
	txState.State = protocol.StateAbort
	delete(p.transactions, txID)

	log.Printf("[Participant %s] Aborted transaction %s", p.node.Addr, txID)
	return &protocol.AbortResponse{
		Success: true,
	}
}

// GetTransactionState returns the current state of a transaction
func (p *Participant) GetTransactionState(txID string) *TransactionState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.transactions[txID]
}

// GetPendingTransactions returns all pending transactions
func (p *Participant) GetPendingTransactions() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	txIDs := make([]string, 0, len(p.transactions))
	for id := range p.transactions {
		txIDs = append(txIDs, id)
	}
	return txIDs
}
