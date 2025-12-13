package protocol

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
}

// NodeInfo contains information about a single node
type NodeInfo struct {
	Address string `json:"address"`
	Role    string `json:"role"`
	Alive   bool   `json:"alive"`
}

// AddNodeRequest is sent to add a new node to the cluster
type AddNodeRequest struct {
	Address string `json:"address"`
}

// AddNodeResponse is returned after adding a node
type AddNodeResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}
