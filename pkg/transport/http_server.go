package transport

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// HTTPServer handles incoming HTTP requests for a node
type HTTPServer struct {
	node          *node.Node
	mux           *http.ServeMux
	server        *http.Server
	onTransaction func(payload any) (*protocol.TransactionResponse, error) // callback for master
}

// NewHTTPServer creates a new HTTP server for a node
func NewHTTPServer(n *node.Node) *HTTPServer {
	s := &HTTPServer{
		node: n,
		mux:  http.NewServeMux(),
	}
	s.setupRoutes()
	return s
}

// SetTransactionHandler sets the callback for handling transaction requests (master only)
func (s *HTTPServer) SetTransactionHandler(handler func(payload any) (*protocol.TransactionResponse, error)) {
	s.onTransaction = handler
}

func (s *HTTPServer) setupRoutes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/role", s.handleRole)
	s.mux.HandleFunc("/prepare", s.handlePrepare)
	s.mux.HandleFunc("/commit", s.handleCommit)
	s.mux.HandleFunc("/abort", s.handleAbort)
	s.mux.HandleFunc("/transaction", s.handleTransaction)
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	s.server = &http.Server{
		Addr:    s.node.Addr,
		Handler: s.mux,
	}

	log.Printf("[HTTPServer] Starting server on %s", s.node.Addr)
	return s.server.ListenAndServe()
}

// Stop stops the HTTP server
func (s *HTTPServer) Stop() error {
	if s.server != nil {
		return s.server.Close()
	}
	return nil
}

// handleHealth responds to health check requests
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := protocol.HealthResponse{
		Status:  "OK",
		Address: s.node.Addr,
		Role:    string(s.node.GetRole()),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleRole responds with the node's current role
func (s *HTTPServer) handleRole(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := protocol.RoleResponse{
		Role:    string(s.node.GetRole()),
		Address: s.node.Addr,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handlePrepare handles prepare phase requests
func (s *HTTPServer) handlePrepare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.PrepareRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendPrepareResponse(w, protocol.StatusAbort, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("[Node %s] Received prepare request for transaction %s", s.node.Addr, req.TransactionID)

	ready, err := s.node.Prepare(req.TransactionID, req.Payload)
	if !ready || err != nil {
		errMsg := "Prepare failed"
		if err != nil {
			errMsg = err.Error()
		}
		sendPrepareResponse(w, protocol.StatusAbort, errMsg, http.StatusInternalServerError)
		return
	}

	sendPrepareResponse(w, protocol.StatusReady, "", http.StatusOK)
}

func sendPrepareResponse(w http.ResponseWriter, status protocol.PrepareStatus, errMsg string, httpStatus int) {
	resp := protocol.PrepareResponse{
		Status: status,
		Error:  errMsg,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp)
}

// handleCommit handles commit requests
func (s *HTTPServer) handleCommit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.CommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendCommitResponse(w, false, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("[Node %s] Received commit request for transaction %s", s.node.Addr, req.TransactionID)

	if err := s.node.Commit(req.TransactionID); err != nil {
		sendCommitResponse(w, false, err.Error(), http.StatusInternalServerError)
		return
	}

	sendCommitResponse(w, true, "", http.StatusOK)
}

func sendCommitResponse(w http.ResponseWriter, success bool, errMsg string, httpStatus int) {
	resp := protocol.CommitResponse{
		Success: success,
		Error:   errMsg,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp)
}

// handleAbort handles abort requests
func (s *HTTPServer) handleAbort(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.AbortRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendAbortResponse(w, false, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("[Node %s] Received abort request for transaction %s", s.node.Addr, req.TransactionID)

	if err := s.node.Abort(req.TransactionID); err != nil {
		sendAbortResponse(w, false, err.Error(), http.StatusInternalServerError)
		return
	}

	sendAbortResponse(w, true, "", http.StatusOK)
}

func sendAbortResponse(w http.ResponseWriter, success bool, errMsg string, httpStatus int) {
	resp := protocol.AbortResponse{
		Success: success,
		Error:   errMsg,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(resp)
}

// handleTransaction handles 2PC transaction requests (master only)
func (s *HTTPServer) handleTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Only master can handle transactions
	if s.node.GetRole() != protocol.RoleMaster {
		resp := protocol.TransactionResponse{
			Success: false,
			Error:   "This node is not the master",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	var req protocol.TransactionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		resp := protocol.TransactionResponse{
			Success: false,
			Error:   "Invalid request body",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	log.Printf("[Master %s] Received transaction request", s.node.Addr)

	if s.onTransaction == nil {
		resp := protocol.TransactionResponse{
			Success: false,
			Error:   "Transaction handler not configured",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(resp)
		return
	}

	result, err := s.onTransaction(req.Payload)
	if err != nil {
		resp := protocol.TransactionResponse{
			Success: false,
			Error:   err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(resp)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if result.Success {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	json.NewEncoder(w).Encode(result)
}
