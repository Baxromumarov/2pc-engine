package cluster

import (
	"sort"
	"sync"

	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// Cluster manages a collection of nodes
type Cluster struct {
	mu     sync.RWMutex
	nodes  map[string]*node.Node // address -> node
	master *node.Node
}

// NewCluster creates a new cluster
func NewCluster() *Cluster {
	return &Cluster{
		nodes: make(map[string]*node.Node),
	}
}

// AddNode adds a node to the cluster
func (c *Cluster) AddNode(n *node.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[n.Addr] = n
}

// RemoveNode removes a node from the cluster
func (c *Cluster) RemoveNode(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if n, exists := c.nodes[addr]; exists {
		if c.master == n {
			c.master = nil
		}
		delete(c.nodes, addr)
	}
}

// GetNode returns a node by address
func (c *Cluster) GetNode(addr string) *node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.nodes[addr]
}

// GetNodes returns all nodes in the cluster
func (c *Cluster) GetNodes() []*node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*node.Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}

	return nodes
}

// GetAliveNodes returns all alive nodes
func (c *Cluster) GetAliveNodes() []*node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*node.Node, 0)
	for _, n := range c.nodes {
		if n.GetAlive() {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// GetSlaveNodes returns all alive slave nodes
func (c *Cluster) GetSlaveNodes() []*node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	nodes := make([]*node.Node, 0)
	for _, n := range c.nodes {
		if n.GetAlive() && n.GetRole() == protocol.RoleSlave {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// GetMaster returns the current master node
func (c *Cluster) GetMaster() *node.Node {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.master
}

// SetMaster sets the master node
func (c *Cluster) SetMaster(n *node.Node) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset old master
	if c.master != nil {
		c.master.SetRole(protocol.RoleSlave)
	}

	c.master = n
	if n != nil {
		n.SetRole(protocol.RoleMaster)
	}
}

// GetNodeAddresses returns all node addresses sorted
func (c *Cluster) GetNodeAddresses() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	addrs := make([]string, 0, len(c.nodes))
	for addr := range c.nodes {
		addrs = append(addrs, addr)
	}

	sort.Strings(addrs)

	return addrs
}

// Size returns the number of nodes in the cluster
func (c *Cluster) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.nodes)
}

// IsMasterAlive checks if the current master is alive
func (c *Cluster) IsMasterAlive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.master == nil {
		return false
	}

	return c.master.GetAlive()
}

// SetNodeName updates the display name for a node.
func (c *Cluster) SetNodeName(addr, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	n, ok := c.nodes[addr]
	if !ok {
		return false
	}

	n.SetName(name)
	return true
}
