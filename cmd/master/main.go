package main

import (
	"database/sql"
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
	_ "github.com/jackc/pgx/v5/stdlib"
)

func main() {
	addr := flag.String("addr", "localhost:8080", "Address for the master node")
	nodes := flag.String("nodes", "", "Comma-separated list of node addresses")
	heartbeatInterval := flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	coordTimeout := flag.Duration("coord-timeout", 10*time.Second, "2PC coordinator timeout")
	dsn := flag.String("dsn", "", "Postgres DSN (e.g., postgres://user:pass@localhost:5432/db?sslmode=disable). Falls back to POSTGRES_DSN env var.")
	flag.Parse()

	if *nodes == "" {
		log.Fatal("Nodes are required. Use --nodes flag with comma-separated addresses")
	}

	nodeAddrs := strings.Split(*nodes, ",")
	if len(nodeAddrs) == 0 {
		log.Fatal("At least one node address is required")
	}

	log.Printf("Starting master on %s with nodes: %v", *addr, nodeAddrs)

	// Resolve DSN and connect
	effectiveDSN := *dsn
	if effectiveDSN == "" {
		effectiveDSN = os.Getenv("POSTGRES_DSN")
	}
	if effectiveDSN == "" {
		log.Fatal("Postgres DSN is required. Set --dsn or POSTGRES_DSN")
	}

	db, err := sql.Open("pgx", effectiveDSN)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	defer db.Close()

	// Create the local node (candidate for master)
	localNode := node.NewNodeWithDB(*addr, protocol.RoleMaster, db)
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

	// Create the 2PC coordinator (master participates in the transaction)
	coordinator := twophasecommit.NewCoordinator(clstr, localNode, *coordTimeout)

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

	// Set up cluster management handlers
	server.SetJoinHandler(func(addr string) (*protocol.JoinResponse, error) {
		// Add the new node to the cluster
		n := node.NewNode(addr, protocol.RoleSlave)
		n.SetAlive(true)
		clstr.AddNode(n)
		log.Printf("[Master] Node %s joined the cluster", addr)

		// Return cluster info
		masterNode := clstr.GetMaster()
		masterAddr := ""
		if masterNode != nil {
			masterAddr = masterNode.Addr
		}

		return &protocol.JoinResponse{
			Success:      true,
			MasterAddr:   masterAddr,
			ClusterNodes: clstr.GetNodeAddresses(),
		}, nil
	})

	server.SetAddNodeHandler(func(addr string) error {
		n := node.NewNode(addr, protocol.RoleSlave)
		n.SetAlive(true)
		clstr.AddNode(n)
		log.Printf("[Master] Added node %s to cluster", addr)
		return nil
	})

	server.SetClusterInfoHandler(func() *protocol.ClusterInfoResponse {
		nodes := clstr.GetNodes()
		nodeInfos := make([]protocol.NodeInfo, 0, len(nodes))
		for _, n := range nodes {
			nodeInfos = append(nodeInfos, protocol.NodeInfo{
				Address: n.Addr,
				Role:    string(n.GetRole()),
				Alive:   n.GetAlive(),
			})
		}

		masterNode := clstr.GetMaster()
		masterAddr := ""
		if masterNode != nil {
			masterAddr = masterNode.Addr
		}

		return &protocol.ClusterInfoResponse{
			MasterAddr: masterAddr,
			Nodes:      nodeInfos,
		}
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
		db.Close()
		os.Exit(0)
	}()

	// Start the server
	log.Printf("Master candidate listening on %s", *addr)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start master server: %v", err)
	}
}
