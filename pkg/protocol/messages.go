package protocol

import "time"

// PrepareRequest is sent by coordinator to participants
type PrepareRequest struct {
	TransactionID string `json:"transaction_id"`
	Payload       any    `json:"payload"`
}

// PrepareResponse is returned by participants
type PrepareResponse struct {
	Status PrepareStatus `json:"status"` // READY or ABORT
	Error  string        `json:"error,omitempty"`
}

// CommitRequest is sent by coordinator to commit
type CommitRequest struct {
	TransactionID string `json:"transaction_id"`
}

// CommitResponse is returned by participants
type CommitResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// AbortRequest is sent by coordinator to abort
type AbortRequest struct {
	TransactionID string `json:"transaction_id"`
}

// AbortResponse is returned by participants
type AbortResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// HealthResponse is returned by health check endpoint
type HealthResponse struct {
	Status  string `json:"status"`
	Address string `json:"address"`
	Role    string `json:"role"`
}

// RoleResponse returns the current role of the node
type RoleResponse struct {
	Role    string `json:"role"`
	Address string `json:"address"`
}

// TransactionRequest is the CLI request to start a 2PC transaction
type TransactionRequest struct {
	Payload any `json:"payload"`
}

// TransactionResponse is the result of a 2PC transaction
type TransactionResponse struct {
	TransactionID string `json:"transaction_id"`
	Success       bool   `json:"success"`
	Message       string `json:"message,omitempty"`
	Error         string `json:"error,omitempty"`
}

// JoinRequest is sent by a new node to join the cluster
type JoinRequest struct {
	Address string `json:"address"` // The address of the node wanting to join
}

// JoinResponse is returned when a node joins the cluster
type JoinResponse struct {
	Success      bool     `json:"success"`
	MasterAddr   string   `json:"master_addr,omitempty"`   // Current master address
	ClusterNodes []string `json:"cluster_nodes,omitempty"` // All nodes in the cluster
	Error        string   `json:"error,omitempty"`
}

// ClusterInfoResponse returns information about the cluster
type ClusterInfoResponse struct {
	MasterAddr string     `json:"master_addr"`
	Nodes      []NodeInfo `json:"nodes"`
	Generated  time.Time  `json:"generated_at"`
}

// NodeInfo contains information about a single node
type NodeInfo struct {
	Name     string      `json:"name,omitempty"`
	Address  string      `json:"address"`
	Role     string      `json:"role"`
	Alive    bool        `json:"alive"`
	Database string      `json:"database,omitempty"`
	Metrics  NodeMetrics `json:"metrics"`
}

// AddNodeRequest is sent to add a new node to the cluster
type AddNodeRequest struct {
	Address  string `json:"address"`
	Name     string `json:"name,omitempty"`
	Database string `json:"database,omitempty"`
}

// AddNodeResponse is returned after adding a node
type AddNodeResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// RemoveNodeRequest removes a node from the cluster
type RemoveNodeRequest struct {
	Address string `json:"address"`
}

// RemoveNodeResponse is returned after removing a node
type RemoveNodeResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// SetNameRequest sets a display name for a node.
type SetNameRequest struct {
	Address string `json:"address"`
	Name    string `json:"name"`
}

// SetNameResponse is returned after setting a node name.
type SetNameResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// NodeMetrics carries lightweight node telemetry for dashboards/automation.
type NodeMetrics struct {
	Prepared    uint64    `json:"prepared"`
	Committed   uint64    `json:"committed"`
	Aborted     uint64    `json:"aborted"`
	Failed      uint64    `json:"failed"`
	InFlight    int       `json:"in_flight"`
	SuccessRate float64   `json:"success_rate"`
	LastError   string    `json:"last_error,omitempty"`
	LastUpdated time.Time `json:"last_updated"`
}

// ClusterDashboardResponse is a richer view for UIs.
type ClusterDashboardResponse struct {
	MasterAddr string     `json:"master_addr"`
	Nodes      []NodeInfo `json:"nodes"`
	Generated  time.Time  `json:"generated_at"`
}

// TransactionRecord represents a stored distributed transaction row.
type TransactionRecord struct {
	TxID      string    `json:"tx_id"`
	Status    string    `json:"status"`
	Payload   any       `json:"payload,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TransactionListResponse represents a paginated set of transactions.
type TransactionListResponse struct {
	Transactions []TransactionRecord `json:"transactions"`
	Total        int                 `json:"total"`
	Page         int                 `json:"page"`
	Limit        int                 `json:"limit"`
	Address      string              `json:"address"`
	HasDB        bool                `json:"has_db"`
}
