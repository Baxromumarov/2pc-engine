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
}

func startNode() {
	fs := flag.NewFlagSet("start-node", flag.ExitOnError)
	addr := fs.String("addr", "localhost:8081", "Address for the node")
	nodes := fs.String("nodes", "", "Comma-separated list of node addresses (including this node) for election/failover")
	heartbeat := fs.String("heartbeat", "5s", "Heartbeat interval (e.g. 5s)")
	coord := fs.String("coord-timeout", "10s", "2PC coordinator timeout (e.g. 10s)")
	dsn := fs.String("dsn", "", "Postgres DSN (fallback to POSTGRES_DSN env var if empty)")
	fs.Parse(os.Args[2:])

	args := []string{"run", "./cmd/node", fmt.Sprintf("--addr=%s", *addr)}
	if *nodes != "" {
		args = append(args, fmt.Sprintf("--nodes=%s", *nodes))
	}
	args = append(args, fmt.Sprintf("--heartbeat=%s", *heartbeat), fmt.Sprintf("--coord-timeout=%s", *coord))
	if *dsn != "" {
		args = append(args, fmt.Sprintf("--dsn=%s", *dsn))
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
