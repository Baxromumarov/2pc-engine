package cluster

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/baxromumarov/2pc-engine/pkg/node"
	"github.com/baxromumarov/2pc-engine/pkg/protocol"
)

// ClusterState holds the minimal data we persist for names and membership.
type ClusterState struct {
	Nodes     []StoredNode `json:"nodes"`
	Generated time.Time    `json:"generated_at"`
}

// StoredNode is the persisted representation of a node.
type StoredNode struct {
	Address  string `json:"address"`
	Name     string `json:"name,omitempty"`
	Database string `json:"database,omitempty"`
}

// StateStore handles encrypted persistence of cluster state.
type StateStore struct {
	path string
	key  []byte
}

// NewStateStore returns an encrypted state store. If either path or key is empty, nil is returned.
func NewStateStore(path, key string) *StateStore {
	if path == "" || key == "" {
		return nil
	}
	derived := sha256.Sum256([]byte(key))
	return &StateStore{
		path: path,
		key:  derived[:],
	}
}

// SaveCluster captures the current cluster nodes (names + DB labels) and writes them encrypted.
func (s *StateStore) SaveCluster(c *Cluster) error {
	if s == nil {
		return nil
	}
	state := &ClusterState{
		Generated: time.Now(),
	}

	addrs := c.GetNodeAddresses()
	state.Nodes = make([]StoredNode, 0, len(addrs))
	for _, addr := range addrs {
		n := c.GetNode(addr)
		if n == nil {
			continue
		}
		state.Nodes = append(state.Nodes, StoredNode{
			Address:  n.Addr,
			Name:     n.GetName(),
			Database: n.GetDatabase(),
		})
	}

	return s.Save(state)
}

// Save writes an arbitrary cluster state encrypted to disk.
func (s *StateStore) Save(state *ClusterState) error {
	if s == nil {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}

	plain, err := json.Marshal(state)
	if err != nil {
		return err
	}

	block, err := aes.NewCipher(s.key)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	ciphertext := gcm.Seal(nonce, nonce, plain, nil)
	encoded := base64.StdEncoding.EncodeToString(ciphertext)

	return os.WriteFile(s.path, []byte(encoded), 0o600)
}

// Load reads and decrypts cluster state from disk.
func (s *StateStore) Load() (*ClusterState, error) {
	if s == nil {
		return nil, nil
	}

	content, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	raw, err := base64.StdEncoding.DecodeString(string(content))
	if err != nil {
		return nil, err
	}

	block, err := aes.NewCipher(s.key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(raw) < nonceSize {
		return nil, errors.New("invalid ciphertext")
	}

	nonce := raw[:nonceSize]
	ciphertext := raw[nonceSize:]

	plain, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, err
	}

	var state ClusterState
	if err := json.Unmarshal(plain, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

// ApplyState merges persisted nodes back into the cluster, updating names and DB labels.
func ApplyState(c *Cluster, state *ClusterState, local *node.Node) {
	if c == nil || state == nil {
		return
	}

	for _, sn := range state.Nodes {
		if sn.Address == "" {
			continue
		}

		// Update local node metadata if present.
		if local != nil && sn.Address == local.Addr {
			if sn.Name != "" {
				local.SetName(sn.Name)
			}

			if sn.Database != "" {
				local.SetDatabase(sn.Database)
			}
		}

		n := c.GetNode(sn.Address)
		if n == nil {
			role := protocol.RoleSlave
			if local != nil && sn.Address == local.Addr {
				role = local.GetRole()
			}
			n = node.NewNode(sn.Address, role)
			c.AddNode(n)
		}

		if sn.Name != "" {
			n.SetName(sn.Name)
		}
		
		if sn.Database != "" {
			n.SetDatabase(sn.Database)
		}
		n.SetAlive(true)
	}
}
