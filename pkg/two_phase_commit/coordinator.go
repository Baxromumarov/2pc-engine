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

type prepareOutcome struct {
	includeLocal    bool
	localPrepared   bool
	preparedRemotes []string
	failedNodes     []string
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

	participantAddrs := make([]string, 0, len(remoteParticipants))
	for _, p := range remoteParticipants {
		participantAddrs = append(participantAddrs, p.Addr)
	}

	outcome := c.prepareTransaction(txID, payload, includeLocal, remoteParticipants)
	if len(outcome.failedNodes) > 0 {
		c.abortTransaction(txID, outcome, participantAddrs)

		return &protocol.TransactionResponse{
			TransactionID: txID,
			Success:       false,
			Error:         fmt.Sprintf("Prepare failed for nodes: %v", outcome.failedNodes),
		}, nil
	}

	commitSuccess, totalCommitted := c.commitTransaction(txID, outcome)
	if commitSuccess {
		return &protocol.TransactionResponse{
			TransactionID: txID,
			Success:       true,
			Message:       fmt.Sprintf("Transaction committed on %d nodes", totalCommitted),
		}, nil
	}

	return &protocol.TransactionResponse{
		TransactionID: txID,
		Success:       false,
		Error:         "Some commits failed",
	}, nil
}

func (c *Coordinator) prepareTransaction(
	txID string,
	payload any,
	includeLocal bool,
	remoteParticipants []*node.Node,
) prepareOutcome {
	outcome := prepareOutcome{
		includeLocal: includeLocal,
	}

	if includeLocal {
		ready, err := c.localNode.Prepare(txID, payload)
		if ready && err == nil {
			outcome.localPrepared = true
			log.Printf("[Coordinator] Local node prepared for transaction %s", txID)
		} else {
			outcome.failedNodes = append(outcome.failedNodes, c.localNode.Addr+" (local)")
			log.Printf("[Coordinator] Local node prepare failed for transaction %s: %v", txID, err)
		}
	}

	prepareResults := c.preparePhase(txID, payload, remoteParticipants)
	for _, result := range prepareResults {
		if result.Success {
			outcome.preparedRemotes = append(outcome.preparedRemotes, result.Addr)
			continue
		}

		outcome.failedNodes = append(outcome.failedNodes, result.Addr)
		if result.Error != nil {
			log.Printf("[Coordinator] Prepare failed for %s: %v", result.Addr, result.Error)
		}
	}

	return outcome
}

func (c *Coordinator) commitTransaction(txID string, outcome prepareOutcome) (bool, int) {
	log.Printf("[Coordinator] All participants ready, committing transaction %s", txID)

	localCommitSuccess := true
	if outcome.includeLocal && outcome.localPrepared {
		if err := c.localNode.Commit(txID); err != nil {
			localCommitSuccess = false
			log.Printf("[Coordinator] Local node commit failed for %s: %v", txID, err)
		} else {
			log.Printf("[Coordinator] Local node committed transaction %s", txID)
		}
	}

	commitResults := c.commitPhase(txID, outcome.preparedRemotes)

	commitSuccess := localCommitSuccess
	for _, result := range commitResults {
		if !result.Success {
			commitSuccess = false
			log.Printf("[Coordinator] Commit failed for %s: %v", result.Addr, result.Error)
		}
	}

	totalCommitted := len(outcome.preparedRemotes)
	if outcome.includeLocal && outcome.localPrepared {
		totalCommitted++
	}

	return commitSuccess, totalCommitted
}

func (c *Coordinator) abortTransaction(txID string, outcome prepareOutcome, participantAddrs []string) {
	log.Printf("[Coordinator] Prepare failed for nodes %v, aborting transaction %s", outcome.failedNodes, txID)

	if outcome.includeLocal && outcome.localPrepared {
		if err := c.localNode.Abort(txID); err != nil {
			log.Printf("[Coordinator] Local node abort failed for %s: %v", txID, err)
		}
	}

	c.abortPhase(txID, participantAddrs)
}

// preparePhase sends prepare requests to all participants
func (c *Coordinator) preparePhase(
	txID string,
	payload any,
	participants []*node.Node,
) []PrepareResult {
	results := make([]PrepareResult, len(participants))
	var wg sync.WaitGroup

	wg.Add(len(participants))

	for i, p := range participants {
		idx := i // shadowing for goroutine
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
