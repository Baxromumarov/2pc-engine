package protocol

// TxState represents the state of a transaction
type TxState string

const (
	StateInit    TxState = "INIT"
	StatePrepare TxState = "PREPARE"
	StateReady   TxState = "READY"
	StateCommit  TxState = "COMMIT"
	StateAbort   TxState = "ABORT"
)

// NodeRole represents the role of a node in the cluster
type NodeRole string

const (
	RoleMaster NodeRole = "MASTER"
	RoleSlave  NodeRole = "SLAVE"
)

// PrepareStatus represents the response status from prepare phase
type PrepareStatus string

const (
	StatusReady PrepareStatus = "READY"
	StatusAbort PrepareStatus = "ABORT"
)
