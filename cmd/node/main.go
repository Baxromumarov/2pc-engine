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
	addr := flag.String("addr", "localhost:8081", "Address to bind the node")
	nodes := flag.String("nodes", "", "Comma-separated list of all node addresses (including this one) for election/failover")
	heartbeatInterval := flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	coordTimeout := flag.Duration("coord-timeout", 10*time.Second, "2PC coordinator timeout")
	flag.Parse()

	if *addr == "" {
		log.Fatal("Address is required. Use --addr flag")
	}

	log.Printf("Starting node on %s", *addr)

	// Build cluster membership
	clstr := cluster.NewCluster()
	localNode := node.NewNode(*addr, protocol.RoleSlave)
	localNode.SetAlive(true)
	clstr.AddNode(localNode)

	if *nodes != "" {
		for _, nAddr := range strings.Split(*nodes, ",") {
			nAddr = strings.TrimSpace(nAddr)
			if nAddr == "" || nAddr == *addr {
				continue
			}
			n := node.NewNode(nAddr, protocol.RoleSlave)
			n.SetAlive(true)
			clstr.AddNode(n)
		}
	}

	// Coordinator will only be used when this node is master
	coordinator := twophasecommit.NewCoordinator(clstr, *coordTimeout)

	// Create HTTP server
	server := transport.NewHTTPServer(localNode)
	server.SetTransactionHandler(func(payload any) (*protocol.TransactionResponse, error) {
		if localNode.GetRole() != protocol.RoleMaster {
			return &protocol.TransactionResponse{
				Success: false,
				Error:   "This node is not the master",
			}, nil
		}
		return coordinator.Execute(payload)
	})

	// Start heartbeat manager to track health and elections
	heartbeat := cluster.NewHeartbeatManager(clstr, *heartbeatInterval)
	heartbeat.Start()

	// Trigger an initial election based on current health (will be refined by heartbeat checks)
	clstr.CheckAndElect()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Shutting down node...")
		heartbeat.Stop()
		server.Stop()
		os.Exit(0)
	}()

	// Start the server (blocking)
	log.Printf("Node ready on %s (peers: %s)", *addr, *nodes)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
