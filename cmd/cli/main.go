package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
	"github.com/baxromumarov/2pc-engine/pkg/transport"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "start-node":
		startNode()
	case "start-master":
		startMaster()
	case "commit":
		commit()
	case "health":
		healthCheck()
	case "status":
		clusterStatus()
	case "add-node":
		addNode()
	case "remove-node":
		removeNode()
	case "dashboard":
		dashboard()
	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("2PC CLI Tool")
	fmt.Println("")
	fmt.Println("Usage:")
	fmt.Println("  cli start-node --addr=<address>")
	fmt.Println("      Start a new node on the specified address")
	fmt.Println("")
	fmt.Println("  cli start-master --addr=<address> --nodes=<node1,node2,...>")
	fmt.Println("      Start a master node with the specified slave nodes")
	fmt.Println("")
	fmt.Println("  cli commit --master=<address> --payload=<json>")
	fmt.Println("      Start a distributed transaction via the master")
	fmt.Println("")
	fmt.Println("  cli health --addr=<address>")
	fmt.Println("      Check health of a specific node")
	fmt.Println("")
	fmt.Println("  cli status --nodes=<node1,node2,...>")
	fmt.Println("      Check status of all nodes and find the master")
	fmt.Println("")
	fmt.Println("  cli add-node --master=<address> --addr=<nodeAddress> [--name=<display>] [--database=<dsn>]")
	fmt.Println("      Register a new node with the cluster (node must already be running)")
	fmt.Println("")
	fmt.Println("  cli remove-node --master=<address> --addr=<nodeAddress>")
	fmt.Println("      Remove a node from the cluster membership")
	fmt.Println("")
	fmt.Println("  cli dashboard --master=<address>")
	fmt.Println("      Show a textual dashboard with health/metrics from the master")
}

func startNode() {
	fs := flag.NewFlagSet("start-node", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8081", "Address for the node")
	nodes := fs.String("nodes", "", "Comma-separated list of node addresses (including this node) for election/failover")
	heartbeat := fs.String("heartbeat", "5s", "Heartbeat interval (e.g. 5s)")
	coord := fs.String("coord-timeout", "10s", "2PC coordinator timeout (e.g. 10s)")
	dsn := fs.String("dsn", "", "Postgres DSN (fallback to POSTGRES_DSN env var if empty)")
	name := fs.String("name", "", "Display name for this node (optional)")
	fs.Parse(os.Args[2:])

	args := []string{"run", "./cmd/node", fmt.Sprintf("--addr=%s", *addr)}
	if *nodes != "" {
		args = append(args, fmt.Sprintf("--nodes=%s", *nodes))
	}
	args = append(args, fmt.Sprintf("--heartbeat=%s", *heartbeat), fmt.Sprintf("--coord-timeout=%s", *coord))
	if *dsn != "" {
		args = append(args, fmt.Sprintf("--dsn=%s", *dsn))
	}
	if *name != "" {
		args = append(args, fmt.Sprintf("--name=%s", *name))
	}

	fmt.Printf("Starting node %s...\n", *addr)
	cmd := exec.Command("go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}
}

func startMaster() {
	fs := flag.NewFlagSet("start-master", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8080", "Address for the master")
	nodes := fs.String("nodes", "", "Comma-separated list of node addresses")
	heartbeat := fs.String("heartbeat", "5s", "Heartbeat interval (e.g. 5s)")
	coord := fs.String("coord-timeout", "10s", "2PC coordinator timeout (e.g. 10s)")
	dsn := fs.String("dsn", "", "Postgres DSN (fallback to POSTGRES_DSN env var if empty)")
	name := fs.String("name", "", "Display name for this master (optional)")
	fs.Parse(os.Args[2:])

	if *nodes == "" {
		fmt.Println("Error: --nodes is required")
		os.Exit(1)
	}

	args := []string{
		"run",
		"./cmd/master",
		fmt.Sprintf("--addr=%s", *addr),
		fmt.Sprintf("--nodes=%s", *nodes),
		fmt.Sprintf("--heartbeat=%s", *heartbeat),
		fmt.Sprintf("--coord-timeout=%s", *coord),
	}
	if *dsn != "" {
		args = append(args, fmt.Sprintf("--dsn=%s", *dsn))
	}
	if *name != "" {
		args = append(args, fmt.Sprintf("--name=%s", *name))
	}

	fmt.Printf("Starting master on %s...\n", *addr)

	cmd := exec.Command("go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("Failed to start master: %v", err)
	}
}

func commit() {
	fs := flag.NewFlagSet("commit", flag.ExitOnError)
	master := fs.String("master", "", "Master node address")
	payload := fs.String("payload", "{}", "Transaction payload as JSON")
	nodes := fs.String("nodes", "", "Comma-separated list of node addresses to find master")
	fs.Parse(os.Args[2:])

	client := transport.NewHTTPClient(10 * time.Second)

	// Find master if not specified
	masterAddr := *master
	if masterAddr == "" && *nodes != "" {
		masterAddr = findMaster(client, strings.Split(*nodes, ","))
	}

	if masterAddr == "" {
		log.Fatal("Could not find master. Specify --master or --nodes")
	}

	// Parse payload
	var payloadData any
	if err := json.Unmarshal([]byte(*payload), &payloadData); err != nil {
		log.Fatalf("Invalid JSON payload: %v", err)
	}

	// Send transaction request
	req := &protocol.TransactionRequest{
		Payload: payloadData,
	}

	fmt.Printf("Sending transaction to master at %s...\n", masterAddr)

	resp, err := client.StartTransaction(masterAddr, req)
	if err != nil {
		log.Fatalf("Transaction failed: %v", err)
	}

	// Print result
	if resp.Success {
		fmt.Printf("✓ Transaction %s committed successfully\n", resp.TransactionID)
		if resp.Message != "" {
			fmt.Printf("  Message: %s\n", resp.Message)
		}
	} else {
		fmt.Printf("✗ Transaction %s failed\n", resp.TransactionID)
		if resp.Error != "" {
			fmt.Printf("  Error: %s\n", resp.Error)
		}
	}
}

func healthCheck() {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", "", "Node address to check")
	fs.Parse(os.Args[2:])

	if *addr == "" {
		log.Fatal("--addr is required")
	}

	client := transport.NewHTTPClient(5 * time.Second)

	health, err := client.HealthCheck(*addr)
	if err != nil {
		fmt.Printf("✗ Node %s is DOWN: %v\n", *addr, err)
		os.Exit(1)
	}

	fmt.Printf("✓ Node %s is UP\n", *addr)
	fmt.Printf("  Role: %s\n", health.Role)
	fmt.Printf("  Status: %s\n", health.Status)
}

func clusterStatus() {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	nodes := fs.String("nodes", "", "Comma-separated list of node addresses")
	fs.Parse(os.Args[2:])

	if *nodes == "" {
		log.Fatal("--nodes is required")
	}

	client := transport.NewHTTPClient(5 * time.Second)
	nodeAddrs := strings.Split(*nodes, ",")

	fmt.Println("Cluster Status:")
	fmt.Println("---------------")

	for _, addr := range nodeAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		health, err := client.HealthCheck(addr)
		if err != nil {
			fmt.Printf("  ✗ %s: DOWN\n", addr)
			continue
		}

		roleEmoji := "🔹"
		if health.Role == "MASTER" {
			roleEmoji = "👑"
		}
		fmt.Printf("  %s %s: %s (%s)\n", roleEmoji, addr, health.Status, health.Role)
	}
}

func addNode() {
	fs := flag.NewFlagSet("add-node", flag.ExitOnError)
	master := fs.String("master", "", "Master node address")
	addr := fs.String("addr", "", "Address of the node to add")
	name := fs.String("name", "", "Display name for the node (optional)")
	database := fs.String("database", "", "Database/DSN label for display (optional)")
	fs.Parse(os.Args[2:])

	if *master == "" {
		log.Fatal("--master is required")
	}
	if *addr == "" {
		log.Fatal("--addr is required")
	}

	client := transport.NewHTTPClient(5 * time.Second)
	req := &protocol.AddNodeRequest{
		Address:  *addr,
		Name:     *name,
		Database: *database,
	}

	if _, err := client.AddNode(*master, req); err != nil {
		log.Fatalf("Failed to add node: %v", err)
	}

	fmt.Printf("✓ Added node %s via master %s\n", *addr, *master)
	if *name != "" {
		fmt.Printf("  Name: %s\n", *name)
	}
	if *database != "" {
		fmt.Printf("  Database: %s\n", *database)
	}
}

func removeNode() {
	fs := flag.NewFlagSet("remove-node", flag.ExitOnError)
	master := fs.String("master", "", "Master node address")
	addr := fs.String("addr", "", "Address of the node to remove")
	fs.Parse(os.Args[2:])

	if *master == "" {
		log.Fatal("--master is required")
	}
	if *addr == "" {
		log.Fatal("--addr is required")
	}

	client := transport.NewHTTPClient(5 * time.Second)
	req := &protocol.RemoveNodeRequest{
		Address: *addr,
	}

	if _, err := client.RemoveNode(*master, req); err != nil {
		log.Fatalf("Failed to remove node: %v", err)
	}

	fmt.Printf("✓ Removed node %s via master %s\n", *addr, *master)
}

func dashboard() {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	master := fs.String("master", "", "Master node address")
	fs.Parse(os.Args[2:])

	if *master == "" {
		log.Fatal("--master is required")
	}

	client := transport.NewHTTPClient(5 * time.Second)
	info, err := client.ClusterInfo(*master)
	if err != nil {
		log.Fatalf("Failed to fetch cluster info: %v", err)
	}

	fmt.Println("Cluster Dashboard")
	fmt.Println("-----------------")
	if info.MasterAddr != "" {
		fmt.Printf("Master:   %s\n", info.MasterAddr)
	} else {
		fmt.Println("Master:   unknown")
	}
	if !info.Generated.IsZero() {
		fmt.Printf("Snapshot: %s\n", info.Generated.Format(time.RFC3339))
	}
	fmt.Println("Nodes:")

	for _, n := range info.Nodes {
		status := "DOWN"
		if n.Alive {
			status = "UP"
		}
		fmt.Printf("  - %s [%s] (%s)\n", n.Address, n.Role, status)
		if n.Database != "" {
			fmt.Printf("      DB: %s\n", n.Database)
		}
		fmt.Printf("      Load: %d | Success: %.1f%% | committed=%d aborted=%d failed=%d\n",
			n.Metrics.InFlight,
			n.Metrics.SuccessRate,
			n.Metrics.Committed,
			n.Metrics.Aborted,
			n.Metrics.Failed,
		)
	}
	fmt.Println("")
}

func findMaster(client *transport.HTTPClient, nodes []string) string {
	for _, addr := range nodes {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		role, err := client.GetRole(addr)
		if err != nil {
			continue
		}

		if role.Role == "MASTER" {
			return addr
		}
	}
	return ""
}
