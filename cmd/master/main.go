package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
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
	name := flag.String("name", "", "Display name for this master node (optional)")
	stateFile := flag.String("state-file", "cluster_state.enc", "Path to encrypted cluster state file (optional)")
	stateKey := flag.String("state-key", "", "Encryption key for state file (optional, fallback CLUSTER_STATE_KEY)")
	autoStart := flag.Bool("auto-start-nodes", true, "Automatically launch newly added nodes locally (requires go and DSN)")
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
	if *name != "" {
		localNode.SetName(*name)
	}
	localNode.SetDatabase(maskDSN(effectiveDSN))

	// Create the cluster
	clstr := cluster.NewCluster()
	effectiveStateKey := *stateKey
	if effectiveStateKey == "" {
		effectiveStateKey = os.Getenv("CLUSTER_STATE_KEY")
	}
	stateStore := cluster.NewStateStore(*stateFile, effectiveStateKey)
	if *stateFile != "" && stateStore == nil {
		log.Printf("[Master] Persistence disabled: state key missing (set --state-key or CLUSTER_STATE_KEY)")
	}
	persistState := func() {}
	client := transport.NewHTTPClient(5 * time.Second)

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

	if stateStore != nil {
		if loaded, err := stateStore.Load(); err != nil {
			log.Printf("[Master] Failed to load cluster state: %v", err)
		} else if loaded != nil {
			cluster.ApplyState(clstr, loaded, localNode)
			log.Printf("[Master] Loaded %d nodes from state file", len(loaded.Nodes))
		}

		persistState = func() {
			if err := stateStore.SaveCluster(clstr); err != nil {
				log.Printf("[Master] Failed to persist cluster state: %v", err)
			}
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

	server.SetAddNodeHandler(func(addr, name, database string) error {
		n := node.NewNode(addr, protocol.RoleSlave)
		n.SetAlive(true)
		if name != "" {
			n.SetName(name)
		}
		if database != "" {
			n.SetDatabase(database)
		}
		clstr.AddNode(n)
		log.Printf("[Master] Added node %s to cluster", addr)
		persistState()

		if *autoStart && database != "" {
			go func() {
				if err := launchNodeProcess(addr, database, name, *stateFile, effectiveStateKey, clstr); err != nil {
					log.Printf("[Master] Failed to auto-start node %s: %v", addr, err)
				}
			}()
		}

		return nil
	})

	server.SetRemoveNodeHandler(func(addr string) error {
		clstr.RemoveNode(addr)
		log.Printf("[Master] Removed node %s from cluster", addr)
		clstr.CheckAndElect()
		persistState()
		return nil
	})

	server.SetNameHandler(func(addr, name string) error {
		if ok := clstr.SetNodeName(addr, name); !ok {
			return fmt.Errorf("node %s not found", addr)
		}
		persistState()
		return nil
	})

	server.SetTransactionsHandler(func(addr string, page, limit int, status string) (*protocol.TransactionListResponse, error) {
		target := addr
		if target == "" {
			target = localNode.Addr
		}
		if target == localNode.Addr {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			records, total, err := localNode.ListTransactions(ctx, page, limit, status)
			if err != nil {
				return nil, err
			}
			return &protocol.TransactionListResponse{
				Transactions: records,
				Total:        total,
				Page:         page,
				Limit:        limit,
				Address:      target,
				HasDB:        localNode.HasDB(),
			}, nil
		}

		return client.Transactions(target, page, limit, status)
	})

	server.SetClusterInfoHandler(func() *protocol.ClusterInfoResponse {
		addrs := clstr.GetNodeAddresses()
		nodeInfos := make([]protocol.NodeInfo, 0, len(addrs))
		for _, nodeAddr := range addrs {
			n := clstr.GetNode(nodeAddr)
			if n == nil {
				continue
			}

			// For the local node, use local metrics; for remote nodes, fetch via HTTP
			var metrics protocol.NodeMetrics
			if nodeAddr == *addr {
				metrics = n.Metrics()
			} else {
				if remoteMetrics, err := client.GetMetrics(nodeAddr); err == nil {
					metrics = *remoteMetrics
				}
				// On error, metrics stays zero-valued
			}

			nodeInfos = append(nodeInfos, protocol.NodeInfo{
				Name:     n.GetName(),
				Address:  n.Addr,
				Role:     string(n.GetRole()),
				Alive:    n.GetAlive(),
				Database: n.GetDatabase(),
				Metrics:  metrics,
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
			Generated:  time.Now(),
		}
	})

	// Start heartbeat manager
	heartbeat := cluster.NewHeartbeatManager(clstr, *heartbeatInterval)
	heartbeat.Start()

	// Initial election based on the current view; heartbeat will refine
	clstr.CheckAndElect()
	persistState()

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

func maskDSN(dsn string) string {
	if dsn == "" {
		return ""
	}

	if u, err := url.Parse(dsn); err == nil {
		if u.User != nil {
			username := u.User.Username()
			u.User = url.UserPassword(username, "****")
		}
		return u.String()
	}

	if at := strings.Index(dsn, "@"); at > 0 {
		return "****@" + dsn[at+1:]
	}

	return dsn
}

// launchNodeProcess best-effort starts a local node process using go run.
func launchNodeProcess(addr, dsn, name, stateFile, stateKey string, clstr *cluster.Cluster) error {
	args := []string{"run", "./cmd/node", fmt.Sprintf("--addr=%s", addr)}

	nodes := clstr.GetNodeAddresses()
	args = append(args, fmt.Sprintf("--nodes=%s", strings.Join(nodes, ",")))
	if dsn != "" {
		args = append(args, fmt.Sprintf("--dsn=%s", dsn))
	}
	if name != "" {
		args = append(args, fmt.Sprintf("--name=%s", name))
	}

	// Use per-node state file if default.
	nodeState := stateFile
	if nodeState == "cluster_state.enc" || nodeState == "" {
		safeAddr := strings.ReplaceAll(addr, ":", "_")
		nodeState = fmt.Sprintf("cluster_state_%s.enc", safeAddr)
	}
	args = append(args, fmt.Sprintf("--state-file=%s", nodeState))
	if stateKey != "" {
		args = append(args, fmt.Sprintf("--state-key=%s", stateKey))
	}

	cmd := exec.Command("go", args...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("POSTGRES_DSN=%s", dsn))
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("[Master] Auto-starting node %s with DSN %s", addr, maskDSN(dsn))
	return cmd.Start()
}
