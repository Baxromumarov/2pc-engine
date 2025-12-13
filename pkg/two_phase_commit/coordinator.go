package twophasecommit

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/cluster"
	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
	"github.com/baxromumarov/2pc-engine/pkg/transport"
	"github.com/google/uuid"
)

// Coordinator manages the 2PC protocol from the master's perspective
type Coordinator struct {
	cluster *cluster.Cluster
	client  *transport.HTTPClient
	timeout time.Duration
	mu      sync.Mutex
}

// NewCoordinator creates a new 2PC coordinator
func NewCoordinator(c *cluster.Cluster, timeout time.Duration) *Coordinator {
	return &Coordinator{
		cluster: c,
		client:  transport.NewHTTPClient(timeout),
		timeout: timeout,
	}
}

// PrepareResult holds the result of a prepare request
type PrepareResult struct {
	Addr     string
	Success  bool
	Response *protocol.PrepareResponse
	Error    error
}

// CommitResult holds the result of a commit/abort request
type CommitResult struct {
	Addr    string
	Success bool
	Error   error
}

// Execute runs the 2PC protocol for a transaction
func (c *Coordinator) Execute(payload any) (*protocol.TransactionResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	txID := uuid.New().String()
	log.Printf("[Coordinator] Starting 2PC for transaction %s", txID)

	// Get all alive participant nodes (slaves)
	participants := c.cluster.GetSlaveNodes()
	if len(participants) == 0 {
		return &protocol.TransactionResponse{
			TransactionID: txID,
			Success:       false,
			Error:         "No participants available",
		}, nil
	}

	log.Printf("[Coordinator] Found %d participants for transaction %s", len(participants), txID)

	// Phase 1: Prepare
	prepareResults := c.preparePhase(txID, payload, participants)
	participantAddrs := make([]string, 0, len(participants))
	for _, p := range participants {
		participantAddrs = append(participantAddrs, p.Addr)
	}

	// Check if all participants are ready
	allReady := true
	var failedNodes []string
	var preparedNodes []string

	for _, result := range prepareResults {
		if result.Success && result.Response != nil && result.Response.Status == protocol.StatusReady {
			preparedNodes = append(preparedNodes, result.Addr)
		} else {
			allReady = false
			failedNodes = append(failedNodes, result.Addr)
			if result.Error != nil {
				log.Printf("[Coordinator] Prepare failed for %s: %v", result.Addr, result.Error)
			}
		}
	}

	// Phase 2: Commit or Abort
	if allReady {
		log.Printf("[Coordinator] All participants ready, committing transaction %s", txID)
		commitResults := c.commitPhase(txID, preparedNodes)

		// Check commit results
		commitSuccess := true
		for _, result := range commitResults {
			if !result.Success {
				commitSuccess = false
				log.Printf("[Coordinator] Commit failed for %s: %v", result.Addr, result.Error)
			}
		}

		if commitSuccess {
			return &protocol.TransactionResponse{
				TransactionID: txID,
				Success:       true,
				Message:       fmt.Sprintf("Transaction committed on %d nodes", len(preparedNodes)),
			}, nil
		} else {
			return &protocol.TransactionResponse{
				TransactionID: txID,
				Success:       false,
				Error:         "Some commits failed",
			}, nil
		}
	} else {
		log.Printf("[Coordinator] Prepare failed for nodes %v, aborting transaction %s", failedNodes, txID)
		c.abortPhase(txID, participantAddrs)

		return &protocol.TransactionResponse{
			TransactionID: txID,
			Success:       false,
			Error:         fmt.Sprintf("Prepare failed for nodes: %v", failedNodes),
		}, nil
	}
}

// preparePhase sends prepare requests to all participants
func (c *Coordinator) preparePhase(txID string, payload any, participants []*node.Node) []PrepareResult {
	results := make([]PrepareResult, len(participants))
	var wg sync.WaitGroup
	wg.Add(len(participants))

	for i, p := range participants {
		idx := i
		participant := p
		go func() {
			defer wg.Done()

			req := &protocol.PrepareRequest{
				TransactionID: txID,
				Payload:       payload,
			}

			resp, err := c.client.Prepare(participant.Addr, req)
			results[idx] = PrepareResult{
				Addr:     participant.Addr,
				Success:  err == nil && resp != nil && resp.Status == protocol.StatusReady,
				Response: resp,
				Error:    err,
			}
		}()
	}

	wg.Wait()
	return results
}

// commitPhase sends commit requests to all prepared participants
func (c *Coordinator) commitPhase(txID string, preparedAddrs []string) []CommitResult {
	results := make([]CommitResult, len(preparedAddrs))
	var wg sync.WaitGroup
	wg.Add(len(preparedAddrs))

	for i, addr := range preparedAddrs {
		idx := i
		nodeAddr := addr
		go func() {
			defer wg.Done()

			req := &protocol.CommitRequest{
				TransactionID: txID,
			}

			resp, err := c.client.Commit(nodeAddr, req)
			results[idx] = CommitResult{
				Addr:    nodeAddr,
				Success: err == nil && resp != nil && resp.Success,
				Error:   err,
			}
		}()
	}

	wg.Wait()
	return results
}

// abortPhase sends abort requests to all participants that were part of the prepare phase.
func (c *Coordinator) abortPhase(txID string, participantAddrs []string) []CommitResult {
	if len(participantAddrs) == 0 {
		return nil
	}

	results := make([]CommitResult, len(participantAddrs))
	var wg sync.WaitGroup
	wg.Add(len(participantAddrs))

	for i, addr := range participantAddrs {
		idx := i
		nodeAddr := addr
		go func() {
			defer wg.Done()

			req := &protocol.AbortRequest{
				TransactionID: txID,
			}

			resp, err := c.client.Abort(nodeAddr, req)
			results[idx] = CommitResult{
				Addr:    nodeAddr,
				Success: err == nil && resp != nil && resp.Success,
				Error:   err,
			}

			if err != nil {
				log.Printf("[Coordinator] Abort failed for %s: %v", nodeAddr, err)
			}
		}()
	}

	wg.Wait()
	return results
}
