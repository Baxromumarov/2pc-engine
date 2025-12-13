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
	addr := flag.String("addr", "localhost:8081", "Address to bind the node")
	nodes := flag.String("nodes", "", "Comma-separated list of all node addresses (including this one) for election/failover")
	heartbeatInterval := flag.Duration("heartbeat", 5*time.Second, "Heartbeat interval")
	coordTimeout := flag.Duration("coord-timeout", 10*time.Second, "2PC coordinator timeout")
	dsn := flag.String("dsn", "", "Postgres DSN (e.g., postgres://user:pass@localhost:5432/db?sslmode=disable). Falls back to POSTGRES_DSN env var.")
	flag.Parse()

	if *addr == "" {
		log.Fatal("Address is required. Use --addr flag")
	}

	log.Printf("Starting node on %s", *addr)

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

	// Build cluster membership
	clstr := cluster.NewCluster()
	localNode := node.NewNodeWithDB(*addr, protocol.RoleSlave, db)
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
	coordinator := twophasecommit.NewCoordinator(clstr, localNode, *coordTimeout)

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

	// Set up cluster management handlers (same as master, for when this node becomes master)
	server.SetJoinHandler(func(addr string) (*protocol.JoinResponse, error) {
		n := node.NewNode(addr, protocol.RoleSlave)
		n.SetAlive(true)
		clstr.AddNode(n)
		log.Printf("[Node] Node %s joined the cluster", addr)

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
		log.Printf("[Node] Added node %s to cluster", addr)
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
		db.Close()
		os.Exit(0)
	}()

	// Start the server (blocking)
	log.Printf("Node ready on %s (peers: %s)", *addr, *nodes)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
