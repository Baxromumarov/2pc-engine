package cluster

import (
	"testing"

	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

func TestClusterAddRemoveNode(t *testing.T) {
	c := NewCluster()

	// Add nodes
	n1 := node.NewNode("localhost:8081", protocol.RoleSlave)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)

	c.AddNode(n1)
	c.AddNode(n2)

	if c.Size() != 2 {
		t.Errorf("Expected 2 nodes, got %d", c.Size())
	}

	// Remove node
	c.RemoveNode("localhost:8081")
	if c.Size() != 1 {
		t.Errorf("Expected 1 node after removal, got %d", c.Size())
	}

	// Check remaining node
	remaining := c.GetNode("localhost:8082")
	if remaining == nil {
		t.Error("Expected localhost:8082 to still exist")
	}
}

func TestClusterGetAliveNodes(t *testing.T) {
	c := NewCluster()

	n1 := node.NewNode("localhost:8081", protocol.RoleSlave)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)
	n3 := node.NewNode("localhost:8083", protocol.RoleSlave)

	n1.SetAlive(true)
	n2.SetAlive(false)
	n3.SetAlive(true)

	c.AddNode(n1)
	c.AddNode(n2)
	c.AddNode(n3)

	alive := c.GetAliveNodes()
	if len(alive) != 2 {
		t.Errorf("Expected 2 alive nodes, got %d", len(alive))
	}
}

func TestElectMaster(t *testing.T) {
	c := NewCluster()

	// Add nodes in non-sorted order
	n3 := node.NewNode("localhost:8083", protocol.RoleSlave)
	n1 := node.NewNode("localhost:8081", protocol.RoleSlave)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)

	c.AddNode(n3)
	c.AddNode(n1)
	c.AddNode(n2)

	// All nodes alive
	n1.SetAlive(true)
	n2.SetAlive(true)
	n3.SetAlive(true)

	// Elect master - should be the one with lowest address (localhost:8081)
	c.ElectMaster()

	master := c.GetMaster()
	if master == nil {
		t.Fatal("Expected a master to be elected")
	}

	if master.Addr != "localhost:8081" {
		t.Errorf("Expected localhost:8081 to be master (lowest address), got %s", master.Addr)
	}

	if master.GetRole() != protocol.RoleMaster {
		t.Error("Expected master node to have MASTER role")
	}

	// Check others are slaves
	if n2.GetRole() != protocol.RoleSlave {
		t.Error("Expected n2 to be SLAVE")
	}
	if n3.GetRole() != protocol.RoleSlave {
		t.Error("Expected n3 to be SLAVE")
	}
}

func TestElectMasterAfterFailure(t *testing.T) {
	c := NewCluster()

	n1 := node.NewNode("localhost:8081", protocol.RoleSlave)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)

	n1.SetAlive(true)
	n2.SetAlive(true)

	c.AddNode(n1)
	c.AddNode(n2)

	// Initial election
	c.ElectMaster()
	if c.GetMaster().Addr != "localhost:8081" {
		t.Fatalf("Expected localhost:8081 to be initial master")
	}

	// Simulate master failure
	n1.SetAlive(false)

	// Check and elect should detect dead master and elect new one
	elected := c.CheckAndElect()
	if !elected {
		t.Error("Expected a new master to be elected")
	}

	newMaster := c.GetMaster()
	if newMaster == nil {
		t.Fatal("Expected a new master")
	}

	if newMaster.Addr != "localhost:8082" {
		t.Errorf("Expected localhost:8082 to be new master, got %s", newMaster.Addr)
	}
}

func TestNoMasterWhenAllDead(t *testing.T) {
	c := NewCluster()

	n1 := node.NewNode("localhost:8081", protocol.RoleSlave)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)

	n1.SetAlive(false)
	n2.SetAlive(false)

	c.AddNode(n1)
	c.AddNode(n2)

	c.ElectMaster()

	if c.GetMaster() != nil {
		t.Error("Expected no master when all nodes are dead")
	}
}

func TestGetSlaveNodes(t *testing.T) {
	c := NewCluster()

	n1 := node.NewNode("localhost:8081", protocol.RoleMaster)
	n2 := node.NewNode("localhost:8082", protocol.RoleSlave)
	n3 := node.NewNode("localhost:8083", protocol.RoleSlave)

	n1.SetAlive(true)
	n2.SetAlive(true)
	n3.SetAlive(true)

	c.AddNode(n1)
	c.AddNode(n2)
	c.AddNode(n3)
	c.SetMaster(n1)

	slaves := c.GetSlaveNodes()
	if len(slaves) != 2 {
		t.Errorf("Expected 2 slave nodes, got %d", len(slaves))
	}

	for _, s := range slaves {
		if s.GetRole() != protocol.RoleSlave {
			t.Error("Expected all returned nodes to be slaves")
		}
	}
}
