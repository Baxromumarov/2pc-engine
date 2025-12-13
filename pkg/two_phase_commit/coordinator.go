package twophasecommit

import (
	"errors"
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
	cluster   *cluster.Cluster
	localNode *node.Node // The local (master) node that also participates
	client    *transport.HTTPClient
	timeout   time.Duration
	mu        sync.Mutex
}

// NewCoordinator creates a new 2PC coordinator
func NewCoordinator(c *cluster.Cluster, localNode *node.Node, timeout time.Duration) *Coordinator {
	return &Coordinator{
		cluster:   c,
		localNode: localNode,
		client:    transport.NewHTTPClient(timeout),
		timeout:   timeout,
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
	remoteParticipants := c.cluster.GetSlaveNodes()

	// Calculate total participants (remote slaves + local master if it has a DB)
	totalParticipants := len(remoteParticipants)
	includeLocal := c.localNode != nil
	if includeLocal {
		totalParticipants++
	}

	if totalParticipants == 0 {
		return &protocol.TransactionResponse{
			TransactionID: txID,
			Success:       false,
			Error:         "No participants available",
		}, nil
	}

	log.Printf("[Coordinator] Found %d participants for transaction %s (including local: %v)", totalParticipants, txID, includeLocal)

	// Phase 1: Prepare
	// First, prepare the local node (master)
	var localPrepared bool
	if includeLocal {
		ready, err := c.localNode.Prepare(txID, payload)
		if ready && err == nil {
			localPrepared = true
			log.Printf("[Coordinator] Local node prepared for transaction %s", txID)
		} else {
			log.Printf("[Coordinator] Local node prepare failed for transaction %s: %v", txID, err)
		}
	}

	// Then, prepare remote participants
	prepareResults := c.preparePhase(txID, payload, remoteParticipants)
	participantAddrs := make([]string, 0, len(remoteParticipants))
	for _, p := range remoteParticipants {
		participantAddrs = append(participantAddrs, p.Addr)
	}

	// Check if all participants are ready
	allReady := (!includeLocal || localPrepared) // Local must be ready if included
	var failedNodes []string
	var preparedNodes []string

	if includeLocal && !localPrepared {
		allReady = false
		failedNodes = append(failedNodes, c.localNode.Addr+" (local)")
	}

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

		// Commit local node first
		var localCommitSuccess bool
		if includeLocal && localPrepared {
			if err := c.localNode.Commit(txID); err != nil {
				log.Printf("[Coordinator] Local node commit failed for %s: %v", txID, err)
				localCommitSuccess = false
			} else {
				localCommitSuccess = true
				log.Printf("[Coordinator] Local node committed transaction %s", txID)
			}
		} else {
			localCommitSuccess = true // Not included or not prepared
		}

		// Commit remote participants
		commitResults := c.commitPhase(txID, preparedNodes)

		// Check commit results
		commitSuccess := localCommitSuccess
		for _, result := range commitResults {
			if !result.Success {
				commitSuccess = false
				log.Printf("[Coordinator] Commit failed for %s: %v", result.Addr, result.Error)
			}
		}

		if commitSuccess {
			totalCommitted := len(preparedNodes)
			if includeLocal && localPrepared {
				totalCommitted++
			}
			return &protocol.TransactionResponse{
				TransactionID: txID,
				Success:       true,
				Message:       fmt.Sprintf("Transaction committed on %d nodes", totalCommitted),
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

		// Abort local node if it was prepared
		if includeLocal && localPrepared {
			if err := c.localNode.Abort(txID); err != nil {
				log.Printf("[Coordinator] Local node abort failed for %s: %v", txID, err)
			}
		}

		// Abort remote participants
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
			if err == nil && resp != nil && !resp.Success && resp.Error != "" {
				err = errors.New(resp.Error)
			}
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
