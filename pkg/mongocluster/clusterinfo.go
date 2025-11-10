package mongocluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// ClusterInfo represents the cluster information to write to file
type ClusterInfo struct {
	ConnectionString  string            `json:"connection_string"`
	ConnectionCommand string            `json:"connection_command,omitempty"` // Shell command to connect (with appropriate binary)
	MongosHosts       []string          `json:"mongos_hosts"`
	DataDirectory     string            `json:"data_directory"`
	LogsDirectory     string            `json:"logs_directory"`
	BasePort          int               `json:"base_port"`
	Ports             []int             `json:"ports"`
	PortMapping       map[string]string `json:"port_mapping"`
	Hosts             []string          `json:"hosts"`
	Auth              *AuthInfo         `json:"auth,omitempty"`
	Version           string            `json:"version"`
	BinPath           string            `json:"bin_path,omitempty"` // MongoDB binary path
	Topology          TopologyInfo      `json:"topology"`
}

// AuthInfo represents authentication information
type AuthInfo struct {
	Enabled  bool   `json:"enabled"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// TopologyInfo represents cluster topology
type TopologyInfo struct {
	Shards        int `json:"shards"`
	ReplicaNodes  int `json:"replica_nodes"`
	ConfigServers int `json:"config_servers"`
	MongosCount   int `json:"mongos_count"`
}

// ReadClusterInfoFromFile reads cluster information from a JSON file
func ReadClusterInfoFromFile(filePath string) (*ClusterInfo, error) {
	// Get absolute path
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Read file
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// Unmarshal JSON
	var info ClusterInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster info: %w", err)
	}

	return &info, nil
}

// WriteClusterInfoToFile writes cluster information to a JSON file
func WriteClusterInfoToFile(filePath string, info *ClusterInfo) error {
	// Get absolute path
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	// Write to file
	if err := os.WriteFile(absPath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}
