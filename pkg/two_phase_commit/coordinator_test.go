package twophasecommit

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/cluster"
	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// TestSuccessful2PC tests the happy path where all nodes prepare and commit successfully
func TestSuccessful2PC(t *testing.T) {
	// Create mock nodes
	node1 := createMockNode(t, true, true) // prepare success, commit success
	node2 := createMockNode(t, true, true)
	defer node1.Close()
	defer node2.Close()

	// Create cluster with mock nodes
	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	// Add slave nodes pointing to mock servers
	slave1 := node.NewNode(node1.Listener.Addr().String(), protocol.RoleSlave)
	slave2 := node.NewNode(node2.Listener.Addr().String(), protocol.RoleSlave)
	slave1.SetAlive(true)
	slave2.SetAlive(true)
	c.AddNode(slave1)
	c.AddNode(slave2)

	// Create coordinator and execute (nil localNode = master doesn't participate)
	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if !resp.Success {
		t.Errorf("Expected success, got failure: %s", resp.Error)
	}

	if resp.TransactionID == "" {
		t.Error("Expected transaction ID to be set")
	}
}

// TestPrepareFails tests when one node fails prepare
func TestPrepareFails(t *testing.T) {
	// Create mock nodes - one fails prepare
	node1 := createMockNode(t, true, true)  // prepare success
	node2 := createMockNode(t, false, true) // prepare fails
	defer node1.Close()
	defer node2.Close()

	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	slave1 := node.NewNode(node1.Listener.Addr().String(), protocol.RoleSlave)
	slave2 := node.NewNode(node2.Listener.Addr().String(), protocol.RoleSlave)
	slave1.SetAlive(true)
	slave2.SetAlive(true)
	c.AddNode(slave1)
	c.AddNode(slave2)

	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.Success {
		t.Error("Expected failure when one node fails prepare")
	}
}

// TestNoParticipants tests when there are no participants available
func TestNoParticipants(t *testing.T) {
	c := cluster.NewCluster()
	master := node.NewNode("localhost:8080", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	coordinator := NewCoordinator(c, nil, 5*time.Second)
	resp, err := coordinator.Execute(map[string]string{"test": "data"})

	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	if resp.Success {
		t.Error("Expected failure when no participants")
	}

	if resp.Error != "No participants available" {
		t.Errorf("Expected 'No participants available' error, got: %s", resp.Error)
	}
}

func TestCoordinator_ExecuteFullFlowWithFailures(t *testing.T) {
	payload := samplePayload()

	t.Run("PartialFailureTriggersAbort", func(t *testing.T) {
		timeout := 75 * time.Millisecond
		success := newStubNodeServer(readyPrepare(0), commitSuccess(), abortSuccess())
		timeoutting := newStubNodeServer(
			readyPrepare(2*timeout), // exceeds coordinator timeout -> client error
			commitSuccess(),
			abortSuccess(),
		)
		defer success.Close()
		defer timeoutting.Close()

		c := testClusterWithSlaves(success.Addr(), timeoutting.Addr())
		local := node.NewNode("local:0", protocol.RoleMaster)
		local.SetAlive(true)

		coordinator := NewCoordinator(c, local, timeout)
		resp, err := coordinator.Execute(payload)
		if err != nil {
			t.Fatalf("Execute() returned error: %v", err)
		}
		if resp.Success {
			t.Fatalf("Execute() = success, expected abort due to timeout: %#v", resp)
		}
		if !strings.Contains(resp.Error, timeoutting.Addr()) {
			t.Errorf("Expected error to mention timed-out node %q, got %q", timeoutting.Addr(), resp.Error)
		}
		if local.TxState != protocol.StateAbort {
			t.Fatalf("Local node state = %s, want ABORT", local.TxState)
		}

		successCalls := success.callCounts()
		timeoutCalls := timeoutting.callCounts()
		if successCalls.commit != 0 || timeoutCalls.commit != 0 {
			t.Fatalf("Commit should not run when prepare fails, got commits: success=%d timeout=%d", successCalls.commit, timeoutCalls.commit)
		}
		if successCalls.abort != 1 || timeoutCalls.abort != 1 {
			t.Fatalf("Abort should be sent to all participants, got aborts: success=%d timeout=%d", successCalls.abort, timeoutCalls.abort)
		}
	})

	t.Run("AllSuccessCommits", func(t *testing.T) {
		timeout := 200 * time.Millisecond
		readyA := newStubNodeServer(readyPrepare(0), commitSuccess(), abortSuccess())
		readyB := newStubNodeServer(readyPrepare(0), commitSuccess(), abortSuccess())
		defer readyA.Close()
		defer readyB.Close()

		c := testClusterWithSlaves(readyA.Addr(), readyB.Addr())
		local := node.NewNode("local:0", protocol.RoleMaster)
		local.SetAlive(true)

		coordinator := NewCoordinator(c, local, timeout)
		resp, err := coordinator.Execute(payload)
		if err != nil {
			t.Fatalf("Execute() returned error: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Execute() failed unexpectedly: %#v", resp)
		}
		expectedMsg := "Transaction committed on 3 nodes"
		if resp.Message != expectedMsg {
			t.Fatalf("Commit message = %q, want %q", resp.Message, expectedMsg)
		}
		if local.TxState != protocol.StateCommit {
			t.Fatalf("Local node state = %s, want COMMIT", local.TxState)
		}

		if calls := readyA.callCounts(); calls.commit != 1 || calls.abort != 0 {
			t.Fatalf("Node A calls: %+v, expected 1 commit and 0 aborts", calls)
		}
		if calls := readyB.callCounts(); calls.commit != 1 || calls.abort != 0 {
			t.Fatalf("Node B calls: %+v, expected 1 commit and 0 aborts", calls)
		}
	})

	t.Run("LocalOnlyParticipant", func(t *testing.T) {
		c := testClusterWithSlaves()
		local := node.NewNode("local:0", protocol.RoleMaster)
		local.SetAlive(true)

		coordinator := NewCoordinator(c, local, 100*time.Millisecond)
		resp, err := coordinator.Execute(payload)
		if err != nil {
			t.Fatalf("Execute() returned error: %v", err)
		}
		if !resp.Success {
			t.Fatalf("Execute() failed unexpectedly: %#v", resp)
		}
		if resp.Message != "Transaction committed on 1 nodes" {
			t.Fatalf("Commit message = %q, want %q", resp.Message, "Transaction committed on 1 nodes")
		}
		if local.TxState != protocol.StateCommit {
			t.Fatalf("Local node state = %s, want COMMIT", local.TxState)
		}
	})

	t.Run("ConcurrentExecuteSerialized", func(t *testing.T) {
		prepareDelay := 100 * time.Millisecond
		timeout := 250 * time.Millisecond
		remoteA := newStubNodeServer(readyPrepare(prepareDelay), commitSuccess(), abortSuccess())
		remoteB := newStubNodeServer(readyPrepare(prepareDelay), commitSuccess(), abortSuccess())
		defer remoteA.Close()
		defer remoteB.Close()

		c := testClusterWithSlaves(remoteA.Addr(), remoteB.Addr())
		coordinator := NewCoordinator(c, nil, timeout)

		start := time.Now()
		var wg sync.WaitGroup
		wg.Add(2)
		errs := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				defer wg.Done()
				resp, err := coordinator.Execute(payload)
				if err != nil {
					errs <- fmt.Errorf("execute call failed: %w", err)
					return
				}
				if resp == nil || !resp.Success {
					errs <- fmt.Errorf("unexpected failure response: %#v", resp)
					return
				}
				errs <- nil
			}()
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Fatalf("concurrent execute error: %v", err)
			}
		}

		elapsed := time.Since(start)
		if elapsed < 2*prepareDelay {
			t.Fatalf("Execute calls ran concurrently; elapsed=%v want at least %v", elapsed, 2*prepareDelay)
		}

		if calls := remoteA.callCounts(); calls.prepare != 2 || calls.commit != 2 {
			t.Fatalf("Node A calls: %+v, expected 2 prepare/commit", calls)
		}
		if calls := remoteB.callCounts(); calls.prepare != 2 || calls.commit != 2 {
			t.Fatalf("Node B calls: %+v, expected 2 prepare/commit", calls)
		}
	})
}

// createMockNode creates a mock HTTP server that simulates a node
func createMockNode(t *testing.T, prepareSuccess, commitSuccess bool) *httptest.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/prepare", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.PrepareResponse{}
		if prepareSuccess {
			resp.Status = protocol.StatusReady
			w.WriteHeader(http.StatusOK)
		} else {
			resp.Status = protocol.StatusAbort
			resp.Error = "Prepare failed"
			w.WriteHeader(http.StatusInternalServerError)
		}
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/commit", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.CommitResponse{Success: commitSuccess}
		if !commitSuccess {
			resp.Error = "Commit failed"
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/abort", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.AbortResponse{Success: true}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		resp := protocol.HealthResponse{Status: "OK", Role: "SLAVE"}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(resp)
	})

	return httptest.NewServer(mux)
}

func samplePayload() node.SQLAction {
	return node.SQLAction{
		Table:     "test_table",
		Operation: "INSERT",
		Values:    map[string]any{"id": 1},
	}
}

func testClusterWithSlaves(slaveAddrs ...string) *cluster.Cluster {
	c := cluster.NewCluster()
	master := node.NewNode("master:0", protocol.RoleMaster)
	master.SetAlive(true)
	c.AddNode(master)
	c.SetMaster(master)

	for _, addr := range slaveAddrs {
		n := node.NewNode(addr, protocol.RoleSlave)
		n.SetAlive(true)
		c.AddNode(n)
	}

	return c
}

type stubCallCounts struct {
	prepare int
	commit  int
	abort   int
}

type stubEndpoint struct {
	delay    time.Duration
	status   int
	response any
}

type stubNodeServer struct {
	server *httptest.Server

	mu           sync.Mutex
	prepareCalls int
	commitCalls  int
	abortCalls   int

	prepare stubEndpoint
	commit  stubEndpoint
	abort   stubEndpoint
}

func newStubNodeServer(prepare, commit, abort stubEndpoint) *stubNodeServer {
	s := &stubNodeServer{
		prepare: prepare,
		commit:  commit,
		abort:   abort,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/prepare", func(w http.ResponseWriter, r *http.Request) {
		s.handle(w, prepare, &s.prepareCalls)
	})
	mux.HandleFunc("/commit", func(w http.ResponseWriter, r *http.Request) {
		s.handle(w, commit, &s.commitCalls)
	})
	mux.HandleFunc("/abort", func(w http.ResponseWriter, r *http.Request) {
		s.handle(w, abort, &s.abortCalls)
	})

	s.server = httptest.NewServer(mux)
	return s
}

func (s *stubNodeServer) handle(w http.ResponseWriter, ep stubEndpoint, counter *int) {
	s.mu.Lock()
	*counter++
	s.mu.Unlock()

	if ep.delay > 0 {
		time.Sleep(ep.delay)
	}

	status := ep.status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)

	if ep.response == nil {
		_ = json.NewEncoder(w).Encode(map[string]any{})
		return
	}

	_ = json.NewEncoder(w).Encode(ep.response)
}

func (s *stubNodeServer) Close() {
	s.server.Close()
}

func (s *stubNodeServer) Addr() string {
	return s.server.Listener.Addr().String()
}

func (s *stubNodeServer) callCounts() stubCallCounts {
	s.mu.Lock()
	defer s.mu.Unlock()

	return stubCallCounts{
		prepare: s.prepareCalls,
		commit:  s.commitCalls,
		abort:   s.abortCalls,
	}
}

func readyPrepare(delay time.Duration) stubEndpoint {
	return stubEndpoint{
		delay:  delay,
		status: http.StatusOK,
		response: protocol.PrepareResponse{
			Status: protocol.StatusReady,
		},
	}
}

func commitSuccess() stubEndpoint {
	return stubEndpoint{
		status: http.StatusOK,
		response: protocol.CommitResponse{
			Success: true,
		},
	}
}

func abortSuccess() stubEndpoint {
	return stubEndpoint{
		status: http.StatusOK,
		response: protocol.AbortResponse{
			Success: true,
		},
	}
}
