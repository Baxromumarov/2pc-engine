package cluster

import (
	"log"
	"sync"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/transport"
)

// HeartbeatManager handles periodic health checks of all nodes
type HeartbeatManager struct {
	cluster  *Cluster
	client   *transport.HTTPClient
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewHeartbeatManager creates a new heartbeat manager
func NewHeartbeatManager(cluster *Cluster, interval time.Duration) *HeartbeatManager {
	return &HeartbeatManager{
		cluster:  cluster,
		client:   transport.NewHTTPClient(2 * time.Second),
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the heartbeat checking loop
func (h *HeartbeatManager) Start() {
	h.wg.Add(1)
	go h.run()
	log.Printf("[Heartbeat] Started with interval %v", h.interval)
}

// Stop stops the heartbeat manager
func (h *HeartbeatManager) Stop() {
	close(h.stopCh)
	h.wg.Wait()
	log.Println("[Heartbeat] Stopped")
}

func (h *HeartbeatManager) run() {
	defer h.wg.Done()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	// Initial check
	h.checkAllNodes()

	for {
		select {
		case <-ticker.C:
			h.checkAllNodes()
		case <-h.stopCh:
			return
		}
	}
}

// checkAllNodes performs health check on all nodes
func (h *HeartbeatManager) checkAllNodes() {
	nodes := h.cluster.GetNodes()
	if len(nodes) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	for _, n := range nodes {
		node := n // capture for goroutine
		go func() {
			defer wg.Done()
			h.checkNode(node.Addr)
		}()
	}

	wg.Wait()

	// After health checks, check if we need to elect a new master
	h.cluster.CheckAndElect()
}

// checkNode performs a health check on a single node
func (h *HeartbeatManager) checkNode(addr string) {
	node := h.cluster.GetNode(addr)
	if node == nil {
		return
	}

	wasAlive := node.GetAlive()

	_, err := h.client.HealthCheck(addr)
	if err != nil {
		node.SetAlive(false)
		if wasAlive {
			log.Printf("[Heartbeat] Node %s is now DEAD: %v", addr, err)
		}
	} else {
		node.SetAlive(true)
		if !wasAlive {
			log.Printf("[Heartbeat] Node %s is now ALIVE", addr)
		}
	}
}

// CheckNode performs a single health check on a specific node (exposed for manual checks)
func (h *HeartbeatManager) CheckNode(addr string) bool {
	h.checkNode(addr)
	node := h.cluster.GetNode(addr)
	if node == nil {
		return false
	}

	return node.GetAlive()
}

// IsNodeAlive checks if a specific node is alive
func (h *HeartbeatManager) IsNodeAlive(addr string) bool {
	_, err := h.client.HealthCheck(addr)

	return err == nil
}
