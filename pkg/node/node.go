package node

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

const ddl = `
			CREATE TABLE IF NOT EXISTS distributed_tx (
				tx_id TEXT PRIMARY KEY,
				payload JSONB NOT NULL,
				status TEXT NOT NULL,
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			);`

const distTx = "distributed_tx"

// Node represents a single node in the distributed system
type Node struct {
	Addr    string            // address of the node (e.g., "localhost:8081")
	Role    protocol.NodeRole // MASTER or SLAVE
	IsAlive bool              // health status
	TxState protocol.TxState  // current transaction state

	// Transaction management
	pendingTx   map[string]*sql.Tx // map of transaction_id -> pending transaction
	pendingData map[string]any     // simulated data storage for transactions
	mu          sync.RWMutex

	// Database connection (optional, for real DB integration)
	db         *sql.DB
	schemaOnce sync.Once
	schemaErr  error
}

// NewNode creates a new node instance
func NewNode(addr string, role protocol.NodeRole) *Node {
	return &Node{
		Addr:        addr,
		Role:        role,
		IsAlive:     true,
		TxState:     protocol.StateInit,
		pendingTx:   make(map[string]*sql.Tx),
		pendingData: make(map[string]any),
	}
}

// NewNodeWithDB creates a new node with database connection
func NewNodeWithDB(addr string, role protocol.NodeRole, db *sql.DB) *Node {
	n := NewNode(addr, role)
	n.db = db
	n.schemaOnce = sync.Once{}
	n.schemaErr = nil

	return n
}

// ensureSchema creates the transactions table if needed
func (n *Node) ensureSchema(ctx context.Context) error {
	if n.db == nil {
		return nil
	}

	n.schemaOnce.Do(func() {
		n.schemaErr = n.ensureSchemaLocked(ctx)
	})

	return n.schemaErr
}

// ensureSchemaLocked performs a robust create-if-missing with a post-check to tolerate races.
func (n *Node) ensureSchemaLocked(ctx context.Context) error {

	exists, err := n.tableExists(ctx, distTx)
	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	if _, err := n.db.ExecContext(ctx, ddl); err != nil {
		// If we raced with another node, re-check: if the table now exists, ignore the error.
		ok, chkErr := n.tableExists(ctx, distTx)
		if chkErr != nil {
			return chkErr
		}

		if ok {
			return nil
		}

		return err
	}
	return nil
}

func (n *Node) tableExists(ctx context.Context, name string) (bool, error) {
	var regclass *string
	if err := n.db.QueryRowContext(ctx, `SELECT to_regclass($1)`, name).Scan(&regclass); err != nil {
		return false, err
	}
	return regclass != nil, nil
}

// SQLAction describes a simple insert/update request
type SQLAction struct {
	Table     string         `json:"table"`
	Operation string         `json:"operation"` // INSERT or UPDATE (case-insensitive); default INSERT
	Values    map[string]any `json:"values"`
	Where     map[string]any `json:"where,omitempty"` // required for UPDATE
}

func parseSQLAction(payload any) (*SQLAction, error) {
	var action SQLAction

	switch v := payload.(type) {
	case nil:
		return nil, errors.New("payload is required")
	case SQLAction:
		action = v
	case *SQLAction:
		if v == nil {
			return nil, errors.New("payload is required")
		}
		action = *v
	case []byte:
		if err := json.Unmarshal(v, &action); err != nil {
			return nil, err
		}
	case string:
		if err := json.Unmarshal([]byte(v), &action); err != nil {
			return nil, err
		}
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(bytes, &action); err != nil {
			return nil, err
		}
	}

	action.Operation = strings.ToUpper(strings.TrimSpace(action.Operation))
	if action.Operation == "" {
		action.Operation = "INSERT"
	}

	action.Table = strings.TrimSpace(action.Table)

	if err := validateSQLAction(&action); err != nil {
		return nil, err
	}

	return &action, nil
}

func validateSQLAction(action *SQLAction) error {
	if action.Table == "" {
		return errors.New("table is required")
	}

	if len(action.Values) == 0 {
		return errors.New("values are required")
	}

	switch action.Operation {
	case "INSERT":
		return nil
	case "UPDATE":
		if len(action.Where) == 0 {
			return errors.New("where is required for UPDATE")
		}
		return nil
	default:
		return errors.New("unsupported operation: " + action.Operation)
	}
}

func (n *Node) applySQLAction(ctx context.Context, tx *sql.Tx, action *SQLAction) error {
	table, err := safeIdent(action.Table)
	if err != nil {
		return err
	}

	switch action.Operation {
	case "INSERT":
		cols := sortedKeys(action.Values)
		colIdents := make([]string, len(cols))
		args := make([]any, len(cols))
		placeholders := make([]string, len(cols))

		for i, c := range cols {
			ident, err := safeIdent(c)
			if err != nil {
				return err
			}

			colIdents[i] = `"` + ident + `"`
			args[i] = action.Values[c]
			placeholders[i] = placeholder(i + 1)
		}

		stmt := "INSERT INTO \"" + table + "\" (" + strings.Join(colIdents, ",") + ") VALUES (" + strings.Join(placeholders, ",") + ")"

		_, err = tx.ExecContext(ctx, stmt, args...)

		return err

	case "UPDATE":
		setCols := sortedKeys(action.Values)
		whereCols := sortedKeys(action.Where)

		if len(whereCols) == 0 {
			return errors.New("where is required for UPDATE")
		}

		setParts := make([]string, len(setCols))
		args := make([]any, 0, len(setCols)+len(whereCols))

		idx := 1

		for i, c := range setCols {
			ident, err := safeIdent(c)
			if err != nil {
				return err
			}

			setParts[i] = `"` + ident + `"=` + placeholder(idx)
			args = append(args, action.Values[c])
			idx++
		}

		whereParts := make([]string, len(whereCols))
		for i, c := range whereCols {
			ident, err := safeIdent(c)
			if err != nil {
				return err
			}
			whereParts[i] = `"` + ident + `"=` + placeholder(idx)
			args = append(args, action.Where[c])
			idx++
		}

		stmt := "UPDATE \"" + table + "\" SET " + strings.Join(setParts, ",") + " WHERE " + strings.Join(whereParts, " AND ")

		_, err := tx.ExecContext(ctx, stmt, args...)

		return err
	default:
		return errors.New("unsupported operation: " + action.Operation)
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func placeholder(idx int) string {
	return "$" + strconv.Itoa(idx)
}

func safeIdent(id string) (string, error) {
	if id == "" {
		return "", errors.New("identifier empty")
	}
	for _, r := range id {
		if !(r == '_' ||
			r == '-' ||
			(r >= '0' && r <= '9') ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z')) {

			return "", errors.New("identifier contains invalid characters")
		}
	}
	// normalize to lower to avoid quoting issues
	return strings.ToLower(id), nil
}

func isAlreadyFinishedErr(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(err.Error(), "already been committed") ||
		strings.Contains(err.Error(), "already been rolled back") ||
		strings.Contains(err.Error(), "already been committed or rolled back")
}

// SetAlive updates the node's alive status
func (n *Node) SetAlive(alive bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsAlive = alive
}

// GetAlive returns the node's alive status
func (n *Node) GetAlive() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.IsAlive
}

// SetRole updates the node's role
func (n *Node) SetRole(role protocol.NodeRole) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.Role = role
}

// GetRole returns the node's current role
func (n *Node) GetRole() protocol.NodeRole {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.Role
}

// Prepare handles the prepare phase of 2PC
// Returns true if ready to commit, false otherwise
func (n *Node) Prepare(txID string, payload any) (bool, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we already have a pending transaction with this ID
	if _, exists := n.pendingData[txID]; exists {
		return false, errors.New("transaction already in progress")
	}

	// If we have a real database connection, start a transaction and persist the payload
	if n.db != nil {
		// Use a timeout context for schema operations but NOT for the transaction itself
		// because cancelling the context would rollback the transaction
		schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer schemaCancel()

		if err := n.ensureSchema(schemaCtx); err != nil {
			log.Printf("[Node %s] Failed to ensure schema: %v", n.Addr, err)
			return false, err
		}

		// Start the transaction with a background context (no timeout)
		// The transaction will be committed or rolled back later in Commit/Abort
		tx, err := n.db.BeginTx(context.Background(), nil)
		if err != nil {
			log.Printf("[Node %s] Failed to begin transaction: %v", n.Addr, err)
			return false, err
		}

		action, err := parseSQLAction(payload)
		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		// Use a timeout context for SQL operations within the transaction
		opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer opCancel()

		if err := n.applySQLAction(opCtx, tx, action); err != nil {
			_ = tx.Rollback()
			return false, err
		}

		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		res, err := tx.ExecContext(opCtx,
			`INSERT INTO distributed_tx (
				tx_id, 
				payload, 
				status
				) VALUES ($1, $2::jsonb, 'PREPARED')`,
			txID, string(payloadBytes),
		)
		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			_ = tx.Rollback()
			return false, err
		}

		if rows == 0 {
			_ = tx.Rollback()
			return false, errors.New("transaction already exists")
		}

		n.pendingTx[txID] = tx
	} else {
		// Store the payload for simulated transaction
		n.pendingData[txID] = payload
	}

	// Track pending payload for visibility/compat
	if n.db != nil {
		n.pendingData[txID] = payload
	}

	n.TxState = protocol.StateReady
	log.Printf("[Node %s] Prepared transaction %s", n.Addr, txID)

	return true, nil
}

// Commit commits the prepared transaction
func (n *Node) Commit(txID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we have a real transaction, commit it
	if tx, exists := n.pendingTx[txID]; exists {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE 
				distributed_tx 
			SET 
				status='COMMITTED', 
				updated_at=NOW() 
			WHERE 
			tx_id=$1`,
			txID,
		); err != nil {
			if !isAlreadyFinishedErr(err) {
				_ = tx.Rollback()
				log.Printf("[Node %s] Failed to update status for %s: %v", n.Addr, txID, err)
				return err
			}
		}

		if err := tx.Commit(); err != nil {
			if !isAlreadyFinishedErr(err) {
				log.Printf("[Node %s] Failed to commit transaction %s: %v", n.Addr, txID, err)
				return err
			}
		}

		// Ensure we only drop it once; committing again should be idempotent
		delete(n.pendingTx, txID)

	} else if n.db != nil {
		// Idempotent handling: mark as committed even if we already applied it
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := n.db.ExecContext(
			ctx,
			`UPDATE 
				distributed_tx 
			SET 
				status='COMMITTED', 
				updated_at=NOW() 
			WHERE tx_id=$1`,
			txID,
		); err != nil {
			log.Printf("[Node %s] Idempotent commit update failed for %s: %v", n.Addr, txID, err)
			return err
		}
	}

	// Clean up simulated data
	delete(n.pendingData, txID)
	n.TxState = protocol.StateCommit

	log.Printf("[Node %s] Committed transaction %s", n.Addr, txID)
	return nil
}

// Abort rolls back the prepared transaction
func (n *Node) Abort(txID string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// If we have a real transaction, rollback
	if tx, exists := n.pendingTx[txID]; exists {
		if err := tx.Rollback(); err != nil {
			if !isAlreadyFinishedErr(err) {
				log.Printf("[Node %s] Failed to rollback transaction %s: %v", n.Addr, txID, err)
				return err
			}
		}
		delete(n.pendingTx, txID)

	} else if n.db != nil {
		// Idempotent rollback path when the tx was already committed/rolled back
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := n.db.ExecContext(
			ctx,
			`UPDATE 
				distributed_tx
			SET 
				status='ABORTED', 
				updated_at=NOW() 
			WHERE 
				tx_id=$1`,
			txID,
		); err != nil {
			log.Printf("[Node %s] Idempotent abort update failed for %s: %v", n.Addr, txID, err)
			return err
		}
	}

	// Clean up simulated data
	delete(n.pendingData, txID)
	n.TxState = protocol.StateAbort

	log.Printf("[Node %s] Aborted transaction %s", n.Addr, txID)
	return nil
}

// HasPendingTransaction checks if a transaction is pending
func (n *Node) HasPendingTransaction(txID string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, exists := n.pendingData[txID]

	return exists
}

// GetPendingTransactions returns all pending transaction IDs
func (n *Node) GetPendingTransactions() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()

	txIDs := make([]string, 0, len(n.pendingData))
	for txID := range n.pendingData {
		txIDs = append(txIDs, txID)
	}

	return txIDs
}
