package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/cluster"
	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
	"github.com/baxromumarov/2pc-engine/pkg/transport"
	twophasecommit "github.com/baxromumarov/2pc-engine/pkg/two_phase_commit"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "Address for the master node")
	nodes := flag.String("nodes", "", "Comma-separated list of node addresses")
	heartbeatInterval := flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	coordTimeout := flag.Duration("coord-timeout", 10*time.Second, "2PC coordinator timeout")
	flag.Parse()

	if *nodes == "" {
		log.Fatal("Nodes are required. Use --nodes flag with comma-separated addresses")
	}

	nodeAddrs := strings.Split(*nodes, ",")
	if len(nodeAddrs) == 0 {
		log.Fatal("At least one node address is required")
	}

	log.Printf("Starting master on %s with nodes: %v", *addr, nodeAddrs)

	// Create the local node (candidate for master)
	localNode := node.NewNode(*addr, protocol.RoleMaster)
	localNode.SetAlive(true)

	// Create the cluster
	clstr := cluster.NewCluster()

	// Add local node to cluster
	clstr.AddNode(localNode)

	// Add all other nodes to cluster (they will be health-checked)
	for _, nodeAddr := range nodeAddrs {
		trimmedAddr := strings.TrimSpace(nodeAddr)
		if trimmedAddr != "" && trimmedAddr != *addr {
			n := node.NewNode(trimmedAddr, protocol.RoleSlave)
			n.SetAlive(true)
			clstr.AddNode(n)
		}
	}

	// Create the 2PC coordinator
	coordinator := twophasecommit.NewCoordinator(clstr, *coordTimeout)

	// Create HTTP server for master candidate
	server := transport.NewHTTPServer(localNode)

	// Set up transaction handler
	server.SetTransactionHandler(func(payload any) (*protocol.TransactionResponse, error) {
		if localNode.GetRole() != protocol.RoleMaster {
			return &protocol.TransactionResponse{
				Success: false,
				Error:   "This node is not the master",
			}, nil
		}
		return coordinator.Execute(payload)
	})

	// Start heartbeat manager
	heartbeat := cluster.NewHeartbeatManager(clstr, *heartbeatInterval)
	heartbeat.Start()

	// Initial election based on the current view; heartbeat will refine
	clstr.CheckAndElect()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down master...")
		heartbeat.Stop()
		server.Stop()
		os.Exit(0)
	}()

	// Start the server
	log.Printf("Master candidate listening on %s", *addr)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start master server: %v", err)
	}
}
