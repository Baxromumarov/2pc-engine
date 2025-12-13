package cluster

import (
	"log"
	"sort"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// ElectMaster performs a deterministic master election
// The alive node with the lowest lexicographical address becomes master
func (c *Cluster) ElectMaster() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.electMasterLocked()
}

// EvictMaster removes the current master (usually after detecting it's dead)
func (c *Cluster) EvictMaster() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.master != nil {
		log.Printf("[Election] Evicting master: %s", c.master.Addr)
		c.master.SetRole(protocol.RoleSlave)
		c.master = nil
	}
}

// CheckAndElect checks if election is needed and performs it
// Returns true if a new master was elected or the master changed
func (c *Cluster) CheckAndElect() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	lowestAlive := c.lowestAliveAddrLocked()
	currentMaster := ""

	if c.master != nil {
		currentMaster = c.master.Addr
		if !c.master.GetAlive() {
			log.Printf("[Election] Master %s is dead, triggering election", c.master.Addr)

			c.master.SetRole(protocol.RoleSlave)
			c.master = nil
			currentMaster = ""
		}
	}

	// If there is no alive node, nothing to elect
	if lowestAlive == "" {
		if c.master != nil {
			c.master.SetRole(protocol.RoleSlave)
			c.master = nil
		}

		return false
	}

	// If no master or current master isn't the lowest alive, elect deterministically
	if c.master == nil || currentMaster != lowestAlive {
		changed := c.electMasterLocked()
		return changed
	}

	return false
}

// ShouldBeMaster checks if a given address should be the master based on election rules
func (c *Cluster) ShouldBeMaster(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Get all alive nodes sorted by address
	var aliveAddrs []string
	for nodeAddr, n := range c.nodes {
		if n.GetAlive() {
			aliveAddrs = append(aliveAddrs, nodeAddr)
		}
	}

	if len(aliveAddrs) == 0 {
		return false
	}

	sort.Strings(aliveAddrs)

	return aliveAddrs[0] == addr
}

// lowestAliveAddrLocked returns the lexicographically smallest alive node address.
// Caller must hold c.mu.
func (c *Cluster) lowestAliveAddrLocked() string {
	var aliveAddrs []string
	for addr, n := range c.nodes {
		if n.GetAlive() {
			aliveAddrs = append(aliveAddrs, addr)
		}
	}

	if len(aliveAddrs) == 0 {
		return ""
	}

	sort.Strings(aliveAddrs)

	return aliveAddrs[0]
}

// electMasterLocked elects a master based on current alive nodes.
// Caller must hold c.mu.
func (c *Cluster) electMasterLocked() bool {
	lowestAlive := c.lowestAliveAddrLocked()
	if lowestAlive == "" {
		log.Println("[Election] No alive nodes, no master elected")
		c.master = nil
		return false
	}

	// Reset all roles to slave
	for _, n := range c.nodes {
		n.SetRole(protocol.RoleSlave)
	}

	newMaster := c.nodes[lowestAlive]
	newMaster.SetRole(protocol.RoleMaster)
	c.master = newMaster

	log.Printf("[Election] Elected new master: %s", lowestAlive)

	return true
}
