package mlaunch

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/mod/semver"
)

// ProcessInfo tracks information about a running MongoDB process
type ProcessInfo struct {
	Type    string // "mongod", "mongos", "config"
	Port    int
	PID     int
	DataDir string
	LogFile string
	ReplSet string // Replica set name (for mongod)
	Shard   string // Shard name (for mongod in sharded cluster)
	Host    string // host:port
}

// ClusterMetadata stores cluster configuration and process information
type ClusterMetadata struct {
	Processes    []*ProcessInfo `json:"processes"`
	BinPath      string         `json:"binPath,omitempty"`      // MongoDB binary path
	MongoVersion string         `json:"mongoVersion,omitempty"` // MongoDB version
	Auth         bool           `json:"auth,omitempty"`         // Whether auth is enabled
	KeyFile      string         `json:"keyFile,omitempty"`      // Path to keyfile (if auth enabled)
}

// NativeLauncher is a native Go implementation of mlaunch functionality
type NativeLauncher struct {
	config       Config
	dataDir      string
	processes    []*ProcessInfo
	processMu    sync.Mutex
	metadataFile string
	keyFile      string // Path to keyfile for replica set authentication
}

// safeDisconnect disconnects a MongoDB client, ignoring common harmless errors
// This is safe because these errors are just cleanup operations and don't affect functionality
func safeDisconnect(client *mongo.Client, ctx context.Context) {
	if client == nil {
		return
	}
	err := client.Disconnect(ctx)
	if err != nil {
		errStr := err.Error()
		// Ignore common harmless errors:
		// - Authorization errors from endSessions (cleanup operation)
		// - Client already disconnected (can happen during cleanup)
		if strings.Contains(errStr, "not authorized") ||
			strings.Contains(errStr, "Unauthorized") ||
			strings.Contains(errStr, "endSessions") ||
			strings.Contains(errStr, "client is disconnected") ||
			strings.Contains(errStr, "already disconnected") {
			// Silently ignore - these are expected during cleanup
			return
		}
		// Log other errors but don't fail
		log.Printf("Warning: error disconnecting client: %v", err)
	}
}

// NewNativeLauncher creates a new native launcher
func NewNativeLauncher(config Config) *NativeLauncher {
	// Normalize port configuration
	config = normalizePortConfig(config)

	return &NativeLauncher{
		config:       config,
		dataDir:      config.DataDir,
		processes:    make([]*ProcessInfo, 0),
		metadataFile: filepath.Join(config.DataDir, "processes.json"),
	}
}

// normalizePortConfig normalizes port configuration from various formats
func normalizePortConfig(config Config) Config {
	// If PortRange is set, parse it
	if config.PortRange != "" {
		min, max, err := parsePortRange(config.PortRange)
		if err == nil {
			if config.PortMin == 0 {
				config.PortMin = min
			}
			if config.PortMax == 0 {
				config.PortMax = max
			}
		}
	}

	// If BasePort is set but PortMin is not, use BasePort as PortMin
	if config.BasePort > 0 && config.PortMin == 0 {
		config.PortMin = config.BasePort
	}

	// Default to 27017 if nothing is set
	if config.PortMin == 0 {
		config.PortMin = 27017
	}

	return config
}

// parsePortRange parses a port range string like "27017-28000" or "27017:28000"
func parsePortRange(rangeStr string) (min, max int, err error) {
	// Try dash separator first
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		// Try colon separator
		parts = strings.Split(rangeStr, ":")
		if len(parts) != 2 {
			return 0, 0, fmt.Errorf("invalid port range format: %s (expected 'start-end' or 'start:end')", rangeStr)
		}
	}

	min, err = strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid min port: %w", err)
	}

	max, err = strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid max port: %w", err)
	}

	if min > max {
		return 0, 0, fmt.Errorf("min port (%d) must be <= max port (%d)", min, max)
	}

	return min, max, nil
}

// getPort calculates the port for a given index, ensuring it's within the range
func (l *NativeLauncher) getPort(offset int) (int, error) {
	port := l.config.PortMin + offset

	// Check if port exceeds maximum (if set)
	if l.config.PortMax > 0 && port > l.config.PortMax {
		return 0, fmt.Errorf("port %d exceeds maximum port %d", port, l.config.PortMax)
	}

	return port, nil
}

// isPortAvailable checks if a port is available for use
func (l *NativeLauncher) isPortAvailable(port int) bool {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		// Port is not in use (connection failed, which is what we want)
		return true
	}
	conn.Close()
	// Port is in use (connection succeeded)
	return false
}

// getAllRequiredPorts collects all ports that will be used for the cluster
func (l *NativeLauncher) getAllRequiredPorts() ([]int, error) {
	ports := make([]int, 0)

	if l.config.Shards > 0 {
		// Sharded cluster - mongos gets lowest ports first
		// Mongos ports
		for i := 0; i < l.config.MongosCount; i++ {
			port, err := l.getPort(i)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate mongos port: %w", err)
			}
			ports = append(ports, port)
		}

		// Config server ports
		for i := 0; i < l.config.ConfigServers; i++ {
			offset := l.config.MongosCount + i
			port, err := l.getPort(offset)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate config server port: %w", err)
			}
			ports = append(ports, port)
		}

		// Shard server ports
		for shard := 1; shard <= l.config.Shards; shard++ {
			for node := 1; node <= l.config.ReplicaNodes; node++ {
				offset := l.config.MongosCount + l.config.ConfigServers + (shard-1)*l.config.ReplicaNodes + (node - 1)
				port, err := l.getPort(offset)
				if err != nil {
					return nil, fmt.Errorf("failed to calculate shard port: %w", err)
				}
				ports = append(ports, port)
			}
		}
	} else if l.config.ReplicaNodes > 0 {
		// Replica set only
		for node := 1; node <= l.config.ReplicaNodes; node++ {
			port, err := l.getPort(node - 1)
			if err != nil {
				return nil, fmt.Errorf("failed to calculate replica port: %w", err)
			}
			ports = append(ports, port)
		}
	} else {
		// Standalone
		port, err := l.getPort(0)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate standalone port: %w", err)
		}
		ports = append(ports, port)
	}

	return ports, nil
}

// checkPortsAvailable checks if all required ports are available
func (l *NativeLauncher) checkPortsAvailable() error {
	log.Println("Checking if required ports are available...")

	ports, err := l.getAllRequiredPorts()
	if err != nil {
		return fmt.Errorf("failed to get required ports: %w", err)
	}

	var inUsePorts []int
	for _, port := range ports {
		if !l.isPortAvailable(port) {
			inUsePorts = append(inUsePorts, port)
		}
	}

	if len(inUsePorts) > 0 {
		return fmt.Errorf("ports already in use: %v. Please stop any processes using these ports or choose different ports", inUsePorts)
	}

	log.Printf("All required ports are available (%d ports checked)", len(ports))
	return nil
}

// Init initializes and starts a MongoDB cluster
func (l *NativeLauncher) Init() error {
	log.Println("Initializing MongoDB cluster...")

	// Check if all required ports are available before starting
	if err := l.checkPortsAvailable(); err != nil {
		return err
	}

	// Create necessary directories
	if err := l.createDirectories(); err != nil {
		return fmt.Errorf("failed to create directories: %w", err)
	}

	// Create keyfile if auth is enabled and we have replica sets
	if l.config.Auth && (l.config.ReplicaNodes > 0 || l.config.Shards > 0 || l.config.ConfigServers > 0) {
		if err := l.createKeyFile(); err != nil {
			return fmt.Errorf("failed to create keyfile: %w", err)
		}
	}

	// Start processes in order: config servers, shard mongods, mongos
	if l.config.Shards > 0 {
		// Sharded cluster
		if err := l.startConfigServers(); err != nil {
			return fmt.Errorf("failed to start config servers: %w", err)
		}

		// Wait for config servers to be ready before starting shard servers
		if err := l.waitForConfigServersReady(); err != nil {
			return fmt.Errorf("failed to wait for config servers: %w", err)
		}

		if err := l.startShardServers(); err != nil {
			return fmt.Errorf("failed to start shard servers: %w", err)
		}

		// Wait for shards to be ready
		if err := l.waitForShardsReady(); err != nil {
			return fmt.Errorf("failed to wait for shards: %w", err)
		}

		// Initialize replica sets
		if err := l.initReplicaSets(); err != nil {
			return fmt.Errorf("failed to initialize replica sets: %w", err)
		}

		// Create admin user if auth is enabled (must be done before starting mongos)
		if l.config.Auth {
			if err := l.createAdminUsers(); err != nil {
				return fmt.Errorf("failed to create admin users: %w", err)
			}
		}

		if err := l.startMongos(); err != nil {
			return fmt.Errorf("failed to start mongos: %w", err)
		}

		// Wait for mongos to be ready
		if err := l.waitForMongosReady(); err != nil {
			return fmt.Errorf("failed to wait for mongos: %w", err)
		}

		// Configure sharded cluster
		if err := l.configureShardedCluster(); err != nil {
			return fmt.Errorf("failed to configure sharded cluster: %w", err)
		}
	} else if l.config.ReplicaNodes > 0 {
		// Replica set only
		if err := l.startReplicaSet(); err != nil {
			return fmt.Errorf("failed to start replica set: %w", err)
		}

		if err := l.waitForReplicaSetReady(); err != nil {
			return fmt.Errorf("failed to wait for replica set: %w", err)
		}

		if err := l.initReplicaSet("rs1", l.getReplicaSetHosts()); err != nil {
			return fmt.Errorf("failed to initialize replica set: %w", err)
		}

		// Create admin user if auth is enabled
		if l.config.Auth {
			if err := l.createAdminUserOnReplicaSet("rs1", l.getReplicaSetHosts()); err != nil {
				return fmt.Errorf("failed to create admin user: %w", err)
			}
		}
	} else {
		// Standalone
		if err := l.startStandalone(); err != nil {
			return fmt.Errorf("failed to start standalone: %w", err)
		}

		// Wait for standalone to be ready
		if err := l.waitForStandaloneReady(); err != nil {
			return fmt.Errorf("failed to wait for standalone: %w", err)
		}

		// Create admin user if auth is enabled
		if l.config.Auth {
			// Get standalone host
			var standaloneHost string
			l.processMu.Lock()
			for _, proc := range l.processes {
				if proc.Type == "standalone" {
					standaloneHost = proc.Host
					break
				}
			}
			l.processMu.Unlock()

			if standaloneHost != "" {
				if err := l.createAdminUser(standaloneHost); err != nil {
					return fmt.Errorf("failed to create admin user: %w", err)
				}
			}
		}
	}

	// Save process metadata
	if err := l.saveProcessMetadata(); err != nil {
		log.Printf("Warning: failed to save process metadata: %v", err)
	}

	log.Println("Cluster initialized successfully")
	return nil
}

// createKeyFile creates a keyfile for replica set authentication
func (l *NativeLauncher) createKeyFile() error {
	// Generate a random key (MongoDB keyfiles should contain base64-encoded random data)
	// MongoDB requires keyfiles to be between 6 and 1024 characters
	// Base64 encoding: 3 bytes → 4 characters, so 768 bytes → 1024 characters (max)
	// Use 600 bytes to leave a margin (600 * 4 / 3 = 800 characters)
	keyBytes := make([]byte, 600)
	if _, err := rand.Read(keyBytes); err != nil {
		return fmt.Errorf("failed to generate random key: %w", err)
	}

	// Encode to base64 (MongoDB keyfiles are base64-encoded)
	keyContent := base64.StdEncoding.EncodeToString(keyBytes)

	// Create keyfile path
	keyFile := filepath.Join(l.dataDir, "keyfile")
	absKeyFile, err := filepath.Abs(keyFile)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for keyfile: %w", err)
	}

	// Write keyfile
	if err := os.WriteFile(absKeyFile, []byte(keyContent), 0600); err != nil {
		return fmt.Errorf("failed to write keyfile: %w", err)
	}

	// Set permissions to 600 (read/write for owner only) - required by MongoDB
	// On Windows, chmod may not work, but that's okay
	if runtime.GOOS != "windows" {
		if err := os.Chmod(absKeyFile, 0600); err != nil {
			return fmt.Errorf("failed to set keyfile permissions: %w", err)
		}
	}

	l.keyFile = absKeyFile
	log.Printf("Created keyfile at %s", absKeyFile)
	return nil
}

// createDirectories creates necessary directory structure
func (l *NativeLauncher) createDirectories() error {
	dirs := []string{
		l.dataDir,
		filepath.Join(l.dataDir, "logs"),
	}

	if l.config.Shards > 0 {
		// Create config server directories
		for i := 0; i < l.config.ConfigServers; i++ {
			dirs = append(dirs, filepath.Join(l.dataDir, "configRepl", fmt.Sprintf("rs%d", i+1), "db"))
		}

		// Create shard directories
		for shard := 1; shard <= l.config.Shards; shard++ {
			for node := 1; node <= l.config.ReplicaNodes; node++ {
				dirs = append(dirs, filepath.Join(l.dataDir, fmt.Sprintf("shard%02d", shard), fmt.Sprintf("rs%d", node), "db"))
			}
		}
	} else if l.config.ReplicaNodes > 0 {
		// Create replica set directories
		for node := 1; node <= l.config.ReplicaNodes; node++ {
			dirs = append(dirs, filepath.Join(l.dataDir, "configRepl", fmt.Sprintf("rs%d", node), "db"))
		}
	} else {
		// Standalone
		dirs = append(dirs, filepath.Join(l.dataDir, "single", "db"))
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// startConfigServers starts config server processes
func (l *NativeLauncher) startConfigServers() error {
	log.Printf("Starting %d config server(s)...", l.config.ConfigServers)

	for i := 0; i < l.config.ConfigServers; i++ {
		// Config servers come after mongos
		offset := l.config.MongosCount + i
		port, err := l.getPort(offset)
		if err != nil {
			return fmt.Errorf("failed to calculate port for config server %d: %w", i+1, err)
		}
		dataDir := filepath.Join(l.dataDir, "configRepl", fmt.Sprintf("rs%d", i+1), "db")
		logFile := filepath.Join(l.dataDir, "logs", fmt.Sprintf("config%d.log", i+1))

		proc, err := l.startMongod(port, dataDir, logFile, "config", "configRepl", true)
		if err != nil {
			return fmt.Errorf("failed to start config server %d: %w", i+1, err)
		}

		l.addProcess(proc)
		log.Printf("Config server %d started on port %d (PID: %d)", i+1, port, proc.PID)
	}

	return nil
}

// startShardServers starts shard server processes
func (l *NativeLauncher) startShardServers() error {
	log.Printf("Starting %d shard(s) with %d replica node(s) each...", l.config.Shards, l.config.ReplicaNodes)

	for shard := 1; shard <= l.config.Shards; shard++ {
		shardName := fmt.Sprintf("shard%02d", shard)
		replSetName := fmt.Sprintf("rs%d", shard)

		for node := 1; node <= l.config.ReplicaNodes; node++ {
			// Calculate port: min + mongos + config servers + (shard-1)*replicaNodes + (node-1)
			offset := l.config.MongosCount + l.config.ConfigServers + (shard-1)*l.config.ReplicaNodes + (node - 1)
			port, err := l.getPort(offset)
			if err != nil {
				return fmt.Errorf("failed to calculate port for shard %d node %d: %w", shard, node, err)
			}
			dataDir := filepath.Join(l.dataDir, shardName, fmt.Sprintf("rs%d", node), "db")
			logFile := filepath.Join(l.dataDir, "logs", fmt.Sprintf("%s-rs%d.log", shardName, node))

			proc, err := l.startMongod(port, dataDir, logFile, "shard", replSetName, false)
			if err != nil {
				return fmt.Errorf("failed to start shard %d node %d: %w", shard, node, err)
			}

			proc.Shard = shardName
			l.addProcess(proc)
			log.Printf("Shard %d node %d started on port %d (PID: %d)", shard, node, port, proc.PID)
		}
	}

	return nil
}

// startReplicaSet starts replica set processes
func (l *NativeLauncher) startReplicaSet() error {
	log.Printf("Starting replica set with %d node(s)...", l.config.ReplicaNodes)

	for node := 1; node <= l.config.ReplicaNodes; node++ {
		port, err := l.getPort(node - 1)
		if err != nil {
			return fmt.Errorf("failed to calculate port for replica node %d: %w", node, err)
		}
		dataDir := filepath.Join(l.dataDir, "configRepl", fmt.Sprintf("rs%d", node), "db")
		logFile := filepath.Join(l.dataDir, "logs", fmt.Sprintf("rs%d.log", node))

		proc, err := l.startMongod(port, dataDir, logFile, "replica", "rs1", false)
		if err != nil {
			return fmt.Errorf("failed to start replica node %d: %w", node, err)
		}

		l.addProcess(proc)
		log.Printf("Replica node %d started on port %d (PID: %d)", node, port, proc.PID)
	}

	return nil
}

// startStandalone starts a standalone MongoDB instance
func (l *NativeLauncher) startStandalone() error {
	log.Println("Starting standalone MongoDB instance...")

	port, err := l.getPort(0)
	if err != nil {
		return fmt.Errorf("failed to calculate port for standalone: %w", err)
	}
	dataDir := filepath.Join(l.dataDir, "single", "db")
	logFile := filepath.Join(l.dataDir, "logs", "mongod.log")

	proc, err := l.startMongod(port, dataDir, logFile, "standalone", "", false)
	if err != nil {
		return fmt.Errorf("failed to start standalone: %w", err)
	}

	l.addProcess(proc)
	log.Printf("Standalone instance started on port %d (PID: %d)", port, proc.PID)
	return nil
}

// startMongod starts a mongod process
func (l *NativeLauncher) startMongod(port int, dataDir, logFile, procType, replSetName string, isConfig bool) (*ProcessInfo, error) {
	mongodPath := filepath.Join(l.config.BinPath, "mongod")
	if runtime.GOOS == "windows" {
		mongodPath += ".exe"
	}

	// Ensure data directory path is absolute
	absDataDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for data directory: %w", err)
	}

	// Ensure data directory exists
	if err := os.MkdirAll(absDataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory %s: %w", absDataDir, err)
	}

	// Ensure log file path is absolute
	absLogFile, err := filepath.Abs(logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for log file: %w", err)
	}

	// Ensure log directory exists
	logDir := filepath.Dir(absLogFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	// Create empty log file if it doesn't exist (MongoDB will append to it)
	if _, err := os.Stat(absLogFile); os.IsNotExist(err) {
		if f, err := os.Create(absLogFile); err != nil {
			return nil, fmt.Errorf("failed to create log file %s: %w", absLogFile, err)
		} else {
			f.Close()
		}
	}

	args := []string{
		"--port", strconv.Itoa(port),
		"--dbpath", absDataDir,
		"--logpath", absLogFile,
		"--logappend",
		"--bind_ip", "localhost",
	}

	if isConfig {
		args = append(args, "--configsvr")
		if replSetName != "" {
			args = append(args, "--replSet", replSetName)
		}
	} else if procType == "shard" {
		// Shard servers need --shardsvr flag (required in MongoDB 4.4+)
		// Check version and add flag only for MongoDB 4.4 and above
		if l.config.MongoVersion != "" {
			// Normalize version for semver comparison (e.g., "4.4" -> "v4.4.0")
			version := l.config.MongoVersion
			if !strings.HasPrefix(version, "v") {
				version = "v" + version
			}
			// Ensure we have at least major.minor.patch format
			parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
			if len(parts) == 2 {
				version = "v" + strings.Join(parts, ".") + ".0"
			}
			// Compare with v4.4.0
			if semver.Compare(version, "v4.4.0") >= 0 {
				args = append(args, "--shardsvr")
			}
		} else {
			// If version not specified, assume 4.4+ and add flag (safer default)
			args = append(args, "--shardsvr")
		}
		if replSetName != "" {
			args = append(args, "--replSet", replSetName)
		}
	} else if replSetName != "" {
		args = append(args, "--replSet", replSetName)
	}

	if l.config.Auth {
		args = append(args, "--auth")
		// Add keyfile for replica set authentication (required for inter-node auth)
		if replSetName != "" && l.keyFile != "" {
			args = append(args, "--keyFile", l.keyFile)
		}
	}

	cmd := exec.Command(mongodPath, args...)
	// Note: We don't set cmd.Dir since we're using absolute paths for --dbpath and --logpath

	// Detach stdin/stdout/stderr so process can run in background
	// Since we're using --logpath, stdout/stderr will go to the log file
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	// Log the command for debugging
	log.Printf("Starting mongod: %s %s", mongodPath, strings.Join(args, " "))

	// Start the process
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mongod: %w", err)
	}

	// Wait a bit for process to start
	time.Sleep(500 * time.Millisecond)

	// Check if process is still running
	if cmd.Process == nil {
		return nil, fmt.Errorf("mongod process failed to start")
	}

	proc := &ProcessInfo{
		Type:    procType,
		Port:    port,
		PID:     cmd.Process.Pid,
		DataDir: absDataDir,
		LogFile: absLogFile,
		ReplSet: replSetName,
		Host:    fmt.Sprintf("localhost:%d", port),
	}

	return proc, nil
}

// startMongos starts mongos router processes
func (l *NativeLauncher) startMongos() error {
	log.Printf("Starting %d mongos instance(s)...", l.config.MongosCount)

	// Build config server connection string (config servers come after mongos)
	configHosts := make([]string, l.config.ConfigServers)
	for i := 0; i < l.config.ConfigServers; i++ {
		offset := l.config.MongosCount + i
		port, err := l.getPort(offset)
		if err != nil {
			return fmt.Errorf("failed to calculate config server port: %w", err)
		}
		configHosts[i] = fmt.Sprintf("localhost:%d", port)
	}
	configStr := strings.Join(configHosts, ",")

	for i := 0; i < l.config.MongosCount; i++ {
		// Mongos ports are the lowest (offset 0, 1, 2, ...)
		port, err := l.getPort(i)
		if err != nil {
			return fmt.Errorf("failed to calculate mongos port: %w", err)
		}

		mongosPath := filepath.Join(l.config.BinPath, "mongos")
		if runtime.GOOS == "windows" {
			mongosPath += ".exe"
		}

		logFile := filepath.Join(l.dataDir, "logs", fmt.Sprintf("mongos%d.log", i+1))

		// Ensure log file path is absolute
		absLogFile, err := filepath.Abs(logFile)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for log file: %w", err)
		}

		// Ensure log directory exists
		logDir := filepath.Dir(absLogFile)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory %s: %w", logDir, err)
		}

		// Create empty log file if it doesn't exist (MongoDB will append to it)
		if _, err := os.Stat(absLogFile); os.IsNotExist(err) {
			if f, err := os.Create(absLogFile); err != nil {
				return fmt.Errorf("failed to create log file %s: %w", absLogFile, err)
			} else {
				f.Close()
			}
		}

		args := []string{
			"--port", strconv.Itoa(port),
			"--configdb", fmt.Sprintf("configRepl/%s", configStr),
			"--logpath", absLogFile,
			"--logappend",
			"--bind_ip", "localhost",
		}

		if l.config.Auth {
			// Note: mongos doesn't support --auth flag, but it uses --keyFile for authentication
			// Add keyfile for mongos authentication (required for sharded cluster auth)
			if l.keyFile != "" {
				args = append(args, "--keyFile", l.keyFile)
			}
		}

		cmd := exec.Command(mongosPath, args...)
		// Note: We don't set cmd.Dir since we're using absolute paths for --logpath

		// Detach stdin/stdout/stderr so process can run in background
		// Since we're using --logpath, stdout/stderr will go to the log file
		cmd.Stdin = nil
		cmd.Stdout = nil
		cmd.Stderr = nil

		// Log the command for debugging
		log.Printf("Starting mongos %d: %s %s", i+1, mongosPath, strings.Join(args, " "))

		if err := cmd.Start(); err != nil {
			return fmt.Errorf("failed to start mongos %d: %w", i+1, err)
		}

		// Wait longer when auth is enabled (mongos needs to authenticate with config servers)
		waitTime := 500 * time.Millisecond
		if l.config.Auth {
			waitTime = 3 * time.Second // Give more time for authentication
		}

		// Check process status periodically during wait
		checkInterval := 500 * time.Millisecond
		checks := int(waitTime / checkInterval)
		for j := 0; j < checks; j++ {
			time.Sleep(checkInterval)

			if cmd.Process == nil {
				// Process never started, check log file
				if logContent, err := os.ReadFile(absLogFile); err == nil {
					lines := strings.Split(string(logContent), "\n")
					lastLines := lines
					if len(lines) > 10 {
						lastLines = lines[len(lines)-10:]
					}
					return fmt.Errorf("mongos %d failed to start. Last log lines:\n%s", i+1, strings.Join(lastLines, "\n"))
				}
				return fmt.Errorf("mongos %d failed to start (process is nil)", i+1)
			}

			// Check if process is still running
			if err := l.checkProcessRunning(cmd.Process.Pid); err != nil {
				// Process crashed, check log file for errors
				if logContent, err := os.ReadFile(absLogFile); err == nil {
					lines := strings.Split(string(logContent), "\n")
					lastLines := lines
					if len(lines) > 20 {
						lastLines = lines[len(lines)-20:]
					}
					return fmt.Errorf("mongos %d crashed during startup (PID: %d). Last log lines:\n%s", i+1, cmd.Process.Pid, strings.Join(lastLines, "\n"))
				}
				return fmt.Errorf("mongos %d crashed during startup (PID: %d): %v", i+1, cmd.Process.Pid, err)
			}

			// Check log file for immediate errors (even if process is still running)
			if logContent, err := os.ReadFile(absLogFile); err == nil {
				logStr := string(logContent)
				// Check for common fatal errors
				if strings.Contains(logStr, "F -") || strings.Contains(logStr, "FATAL") ||
					strings.Contains(logStr, "terminating") || strings.Contains(logStr, "exception") {
					lines := strings.Split(logStr, "\n")
					lastLines := lines
					if len(lines) > 20 {
						lastLines = lines[len(lines)-20:]
					}
					return fmt.Errorf("mongos %d encountered fatal error during startup. Last log lines:\n%s", i+1, strings.Join(lastLines, "\n"))
				}
			}
		}

		if cmd.Process == nil {
			return fmt.Errorf("mongos process %d failed to start", i+1)
		}

		// Final check: verify process is still running
		if err := l.checkProcessRunning(cmd.Process.Pid); err != nil {
			// Process crashed, check log file for errors
			if logContent, err := os.ReadFile(absLogFile); err == nil {
				lines := strings.Split(string(logContent), "\n")
				lastLines := lines
				if len(lines) > 20 {
					lastLines = lines[len(lines)-20:]
				}
				return fmt.Errorf("mongos %d crashed after startup wait (PID: %d). Last log lines:\n%s", i+1, cmd.Process.Pid, strings.Join(lastLines, "\n"))
			}
			return fmt.Errorf("mongos %d crashed after startup wait (PID: %d): %v", i+1, cmd.Process.Pid, err)
		}

		proc := &ProcessInfo{
			Type:    "mongos",
			Port:    port,
			PID:     cmd.Process.Pid,
			LogFile: absLogFile,
			Host:    fmt.Sprintf("localhost:%d", port),
		}

		l.addProcess(proc)
		log.Printf("Mongos %d started on port %d (PID: %d)", i+1, port, proc.PID)
	}

	return nil
}

// waitForConfigServersReady waits for config servers to be ready in parallel
func (l *NativeLauncher) waitForConfigServersReady() error {
	log.Println("Waiting for config servers to be ready...")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Collect all config processes
	var configProcs []*ProcessInfo
	l.processMu.Lock()
	for _, proc := range l.processes {
		if proc.Type == "config" {
			configProcs = append(configProcs, proc)
		}
	}
	l.processMu.Unlock()

	if len(configProcs) == 0 {
		return nil
	}

	// Check all config servers in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(configProcs))

	for _, proc := range configProcs {
		wg.Add(1)
		go func(p *ProcessInfo) {
			defer wg.Done()
			log.Printf("Checking config server on port %d (PID: %d)...", p.Port, p.PID)
			// Use auth=false before user creation (MongoDB allows localhost connections without auth)
			if err := l.waitForPort(p, ctx, false); err != nil {
				errChan <- fmt.Errorf("config server on port %d not ready: %w", p.Port, err)
				return
			}
			log.Printf("Config server on port %d is ready", p.Port)
		}(proc)
	}

	// Wait for all checks to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForShardsReady waits for shard servers to be ready in parallel
func (l *NativeLauncher) waitForShardsReady() error {
	log.Println("Waiting for shard servers to be ready...")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Collect all shard processes
	var shardProcs []*ProcessInfo
	l.processMu.Lock()
	for _, proc := range l.processes {
		if proc.Type == "shard" {
			shardProcs = append(shardProcs, proc)
		}
	}
	l.processMu.Unlock()

	if len(shardProcs) == 0 {
		return nil
	}

	// Check all shard servers in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(shardProcs))

	for _, proc := range shardProcs {
		wg.Add(1)
		go func(p *ProcessInfo) {
			defer wg.Done()
			log.Printf("Checking shard server on port %d (PID: %d)...", p.Port, p.PID)
			// Use auth=false before user creation (MongoDB allows localhost connections without auth)
			if err := l.waitForPort(p, ctx, false); err != nil {
				errChan <- fmt.Errorf("shard server on port %d not ready: %w", p.Port, err)
				return
			}
			log.Printf("Shard server on port %d is ready", p.Port)
		}(proc)
	}

	// Wait for all checks to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForReplicaSetReady waits for replica set nodes to be ready in parallel
func (l *NativeLauncher) waitForReplicaSetReady() error {
	log.Println("Waiting for replica set nodes to be ready...")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Collect all replica processes
	var replicaProcs []*ProcessInfo
	l.processMu.Lock()
	for _, proc := range l.processes {
		if proc.Type == "replica" {
			replicaProcs = append(replicaProcs, proc)
		}
	}
	l.processMu.Unlock()

	if len(replicaProcs) == 0 {
		return nil
	}

	// Check all replica nodes in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(replicaProcs))

	for _, proc := range replicaProcs {
		wg.Add(1)
		go func(p *ProcessInfo) {
			defer wg.Done()
			log.Printf("Checking replica node on port %d (PID: %d)...", p.Port, p.PID)
			// Use auth=false before user creation (MongoDB allows localhost connections without auth)
			if err := l.waitForPort(p, ctx, false); err != nil {
				errChan <- fmt.Errorf("replica node on port %d not ready: %w", p.Port, err)
				return
			}
			log.Printf("Replica node on port %d is ready", p.Port)
		}(proc)
	}

	// Wait for all checks to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForMongosReady waits for mongos to be ready in parallel
func (l *NativeLauncher) waitForMongosReady() error {
	log.Println("Waiting for mongos to be ready...")

	// Mongos may take longer to start when auth is enabled (needs to authenticate with config servers)
	timeout := 60 * time.Second
	if l.config.Auth {
		timeout = 120 * time.Second // Give more time when auth is enabled
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Collect all mongos processes
	var mongosProcs []*ProcessInfo
	l.processMu.Lock()
	for _, proc := range l.processes {
		if proc.Type == "mongos" {
			mongosProcs = append(mongosProcs, proc)
		}
	}
	l.processMu.Unlock()

	if len(mongosProcs) == 0 {
		return nil
	}

	// Check all mongos instances in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(mongosProcs))

	for _, proc := range mongosProcs {
		wg.Add(1)
		go func(p *ProcessInfo) {
			defer wg.Done()
			log.Printf("Checking mongos on port %d (PID: %d)...", p.Port, p.PID)
			// Use auth=true for mongos (users should already be created before mongos starts)
			useAuth := l.config.Auth
			if err := l.waitForPort(p, ctx, useAuth); err != nil {
				errChan <- fmt.Errorf("mongos on port %d not ready: %w", p.Port, err)
				return
			}
			log.Printf("Mongos on port %d is ready", p.Port)
		}(proc)
	}

	// Wait for all checks to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// waitForPort waits for a port to become available and MongoDB to be ready
// useAuth indicates whether to use authentication credentials (false before user creation, true after)
func (l *NativeLauncher) waitForPort(proc *ProcessInfo, ctx context.Context, useAuth bool) error {
	port := proc.Port
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	addr := fmt.Sprintf("localhost:%d", port)
	connStr := fmt.Sprintf("mongodb://%s", addr)

	// Add authentication if enabled AND useAuth is true
	// Before user creation, MongoDB allows localhost connections without auth even when --auth is enabled
	if l.config.Auth && useAuth {
		username := l.config.Username
		password := l.config.Password
		if username == "" {
			username = "admin"
		}
		if password == "" {
			password = "admin"
		}
		connStr = fmt.Sprintf("mongodb://%s:%s@%s/admin", username, password, addr)
	}

	pingCount := 0
	for {
		select {
		case <-ctx.Done():
			// Check if process is still running
			if proc != nil {
				if err := l.checkProcessRunning(proc.PID); err != nil {
					return fmt.Errorf("timeout waiting for port %d (process may have crashed: %v)", port, err)
				}
			}
			return fmt.Errorf("timeout waiting for port %d", port)
		case <-ticker.C:
			pingCount++

			// Check if process is still running
			if proc != nil {
				if err := l.checkProcessRunning(proc.PID); err != nil {
					// Process died, check log file for errors
					if proc.LogFile != "" {
						if logContent, err := os.ReadFile(proc.LogFile); err == nil {
							// Show last few lines of log
							lines := strings.Split(string(logContent), "\n")
							lastLines := lines
							if len(lines) > 10 {
								lastLines = lines[len(lines)-10:]
							}
							return fmt.Errorf("process on port %d crashed. Last log lines:\n%s", port, strings.Join(lastLines, "\n"))
						}
					}
					return fmt.Errorf("process on port %d crashed: %v", port, err)
				}
			}

			// First check if port is open
			conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
			if err != nil {
				// Log every 5 seconds to avoid spam
				if pingCount%5 == 0 {
					log.Printf("  Port %d: TCP connection failed, retrying...", port)
					// If process is still running but port isn't open after many attempts, check log file
					if proc != nil && proc.LogFile != "" && pingCount >= 10 {
						if logContent, err := os.ReadFile(proc.LogFile); err == nil {
							lines := strings.Split(string(logContent), "\n")
							if len(lines) > 0 {
								// Show last few lines of log to help debug
								lastLines := lines
								if len(lines) > 5 {
									lastLines = lines[len(lines)-5:]
								}
								log.Printf("  Port %d: Recent log entries:\n%s", port, strings.Join(lastLines, "\n"))
							}
						}
					}
				}
				continue // Port not open yet, keep waiting
			}
			conn.Close()

			// Port is open, now check if MongoDB is actually ready
			// Try to connect and run a simple command
			// For MongoDB 3.6, use Direct connection to avoid replica set discovery issues
			// For mongos, we can use direct connection since it's not a replica set
			mongoCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			clientOpts := options.Client().
				ApplyURI(connStr).
				SetDirect(true). // Connect directly to this server, don't try to discover replica set
				SetServerSelectionTimeout(3 * time.Second).
				SetConnectTimeout(3 * time.Second)

			client, err := mongo.Connect(mongoCtx, clientOpts)
			if err != nil {
				cancel()
				// Log every 5 seconds to avoid spam
				if pingCount%5 == 0 {
					log.Printf("  Port %d: MongoDB connection failed: %v, retrying...", port, err)
				}
				continue // MongoDB not ready yet, keep waiting
			}

			// Try to ping the server
			// Use nil read preference to ping any available server
			err = client.Ping(mongoCtx, nil)
			if err == nil {
				client.Disconnect(mongoCtx)
				cancel()
				log.Printf("  Port %d: MongoDB ping successful", port)
				return nil // MongoDB is ready!
			}

			// If ping failed, log the error and disconnect
			client.Disconnect(mongoCtx)
			cancel()

			// Log every 5 seconds to avoid spam, but include error details
			if pingCount%5 == 0 {
				log.Printf("  Port %d: MongoDB ping failed: %v, retrying...", port, err)
			}
		}
	}
}

// checkProcessRunning checks if a process is still running
func (l *NativeLauncher) checkProcessRunning(pid int) error {
	if runtime.GOOS == "windows" {
		// On Windows, we can't easily check if process is running with signal 0
		// Just return nil for now (process check will be less reliable on Windows)
		return nil
	}

	// On Unix systems, sending signal 0 checks if process exists
	// syscall.Kill(pid, 0) returns nil if process exists, error if it doesn't
	err := syscall.Kill(pid, 0)
	if err != nil {
		return fmt.Errorf("process not running: %w", err)
	}

	return nil
}

// initReplicaSets initializes all replica sets in parallel
func (l *NativeLauncher) initReplicaSets() error {
	// Collect all replica sets to initialize
	type replSetInit struct {
		name  string
		hosts []string
	}

	var replSets []replSetInit

	// Add config server replica set if needed
	if l.config.ConfigServers > 0 {
		configHosts := l.getConfigServerHosts()
		replSets = append(replSets, replSetInit{
			name:  "configRepl",
			hosts: configHosts,
		})
	}

	// Add shard replica sets
	for shard := 1; shard <= l.config.Shards; shard++ {
		replSetName := fmt.Sprintf("rs%d", shard)
		hosts := l.getShardHosts(shard)
		replSets = append(replSets, replSetInit{
			name:  replSetName,
			hosts: hosts,
		})
	}

	if len(replSets) == 0 {
		return nil
	}

	// Initialize all replica sets in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(replSets))

	for _, rs := range replSets {
		wg.Add(1)
		go func(replSet replSetInit) {
			defer wg.Done()
			if err := l.initReplicaSet(replSet.name, replSet.hosts); err != nil {
				errChan <- fmt.Errorf("failed to init replica set %s: %w", replSet.name, err)
			}
		}(rs)
	}

	// Wait for all initializations to complete
	wg.Wait()
	close(errChan)

	// Check for any errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// initReplicaSet initializes a replica set using MongoDB driver
func (l *NativeLauncher) initReplicaSet(replSetName string, hosts []string) error {
	if len(hosts) == 0 {
		return fmt.Errorf("no hosts provided for replica set %s", replSetName)
	}

	log.Printf("Initializing replica set %s with hosts: %v", replSetName, hosts)

	// Connect directly to the node with the lowest index (which will become the primary)
	// The primary is typically the node with _id: 0 (first in the members list)
	// Use SetDirect(true) to connect directly, not through replica set discovery
	// MongoDB allows localhost connections without auth to create the first user
	// Replicaset style discovery fails in this case because we're connecting to a single host
	primaryHost := hosts[0] // First host has _id: 0, which will be the primary
	connStr := fmt.Sprintf("mongodb://%s", primaryHost)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use direct connection to avoid replica set discovery issues
	client, err := mongo.Connect(ctx, options.Client().
		ApplyURI(connStr).
		SetDirect(true).
		SetServerSelectionTimeout(10*time.Second).
		SetConnectTimeout(10*time.Second))
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer safeDisconnect(client, ctx)

	// Check if replica set is already initialized
	var status bson.M
	err = client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&status)
	if err == nil {
		log.Printf("Replica set %s already initialized", replSetName)
		return nil
	}

	// Build members list
	members := make([]bson.M, len(hosts))
	for i, host := range hosts {
		members[i] = bson.M{
			"_id":  i,
			"host": host,
		}
	}

	// Initialize replica set using direct driver call
	// Use replSetInitiate command instead of shelling out
	// Use bson.D for ordered document (as per MongoDB driver best practices)
	cmd := bson.D{
		{Key: "replSetInitiate", Value: bson.M{
			"_id":     replSetName,
			"version": 1,
			"members": members,
		}},
	}

	// Use .Err() directly instead of decoding result (as per example)
	err = client.Database("admin").RunCommand(ctx, cmd).Err()
	if err != nil {
		// Check if it's already initialized
		errStr := err.Error()
		if strings.Contains(errStr, "already initialized") || strings.Contains(errStr, "already has") || strings.Contains(errStr, "already been initiated") {
			log.Printf("Replica set %s already initialized", replSetName)
			return nil
		}
		// Skip RSGhost errors - these are transient and will be handled by the wait loop
		if strings.Contains(errStr, "RSGhost") || strings.Contains(errStr, "server selection error") {
			log.Printf("Replica set %s initialization returned RSGhost error (will wait for topology to stabilize): %v", replSetName, err)
			// Continue to wait loop - don't fail yet
		} else {
			return fmt.Errorf("failed to initialize replica set: %w", err)
		}
	}

	// Disconnect and wait for replica set to initialize
	client.Disconnect(ctx)

	// Wait for primary to be elected by polling replSetGetStatus
	log.Printf("Waiting for primary to be elected in replica set %s...", replSetName)

	// Create a new context with longer timeout for waiting
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer waitCancel()

	maxRetries := 60
	retryInterval := 2 * time.Second

	// First, wait a bit for replSetInitiate to propagate
	time.Sleep(3 * time.Second)

	for i := 0; i < maxRetries; i++ {
		// Check if context is cancelled
		if waitCtx.Err() != nil {
			return fmt.Errorf("context cancelled while waiting for primary: %w", waitCtx.Err())
		}

		// Wait before checking
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timeout waiting for primary election in replica set %s", replSetName)
		case <-time.After(retryInterval):
		}

		// Try to connect directly to first host to check status
		checkConnStr := fmt.Sprintf("mongodb://%s", hosts[0])
		checkClient, err := mongo.Connect(waitCtx, options.Client().ApplyURI(checkConnStr).SetDirect(true))
		if err != nil {
			log.Printf("Waiting for replica set to initialize... (attempt %d/%d)", i+1, maxRetries)
			continue
		}

		// Check replica set status
		var status bson.M
		err = checkClient.Database("admin").RunCommand(waitCtx, bson.M{"replSetGetStatus": 1}).Decode(&status)
		safeDisconnect(checkClient, waitCtx)

		if err != nil {
			log.Printf("Waiting for replica set to initialize... (attempt %d/%d)", i+1, maxRetries)
			continue
		}

		// Check if we have a primary and all members are in valid states
		if members, ok := status["members"].(bson.A); ok {
			hasPrimary := false
			allValid := true
			allReady := true
			validStates := map[string]bool{
				"PRIMARY":   true,
				"SECONDARY": true,
				"ARBITER":   true,
			}
			readyStates := map[string]bool{
				"PRIMARY":   true,
				"SECONDARY": true,
			}

			for _, m := range members {
				if member, ok := m.(bson.M); ok {
					stateStr, _ := member["stateStr"].(string)
					if stateStr == "PRIMARY" {
						hasPrimary = true
					}
					// Check if member is in an invalid state (STARTUP, STARTUP2, RSGhost, etc.)
					if !validStates[stateStr] && stateStr != "" {
						allValid = false
						log.Printf("Member %v is in state %s (attempt %d/%d)", member["name"], stateStr, i+1, maxRetries)
					}
					// Check if member is in a ready state (PRIMARY or SECONDARY, not ARBITER)
					// ARBITER is valid but not "ready" for data operations
					if !readyStates[stateStr] && stateStr != "" {
						allReady = false
					}
				}
			}

			// Only proceed if we have a primary, all members are valid, and all data members are ready
			if hasPrimary && allValid && allReady {
				// Wait a bit more for topology to stabilize
				time.Sleep(3 * time.Second)

				// Now verify that we can actually connect using the replica set connection string
				// This ensures the replica set topology is ready (not RSGhost)
				// Try multiple times as topology discovery can take time
				// Be tolerant of initial failures - only fail if we've exhausted all retries
				replSetConnStr := fmt.Sprintf("mongodb://%s/?replicaSet=%s", strings.Join(hosts, ","), replSetName)
				connectionVerified := false
				lastError := error(nil)

				for verifyAttempt := 0; verifyAttempt < 5; verifyAttempt++ {
					replSetClient, err := mongo.Connect(waitCtx, options.Client().ApplyURI(replSetConnStr).SetServerSelectionTimeout(10*time.Second))
					if err == nil {
						// Try to ping to verify connection works
						pingCtx, pingCancel := context.WithTimeout(waitCtx, 10*time.Second)
						err = replSetClient.Ping(pingCtx, nil)
						pingCancel()
						safeDisconnect(replSetClient, waitCtx)

						if err == nil {
							connectionVerified = true
							break
						}
						lastError = err
						// Only log failures on later attempts (be tolerant of early failures)
						if verifyAttempt >= 2 {
							log.Printf("Replica set connection ping failed (verify attempt %d/5): %v", verifyAttempt+1, err)
						}
					} else {
						lastError = err
						// Only log failures on later attempts (be tolerant of early failures)
						if verifyAttempt >= 2 {
							log.Printf("Replica set connection failed (verify attempt %d/5): %v", verifyAttempt+1, err)
						}
					}

					// Wait before retrying (longer wait for later attempts)
					if verifyAttempt < 4 {
						waitTime := 2 * time.Second
						if verifyAttempt >= 2 {
							waitTime = 3 * time.Second // Wait longer for later attempts
						}
						time.Sleep(waitTime)
					}
				}

				if connectionVerified {
					log.Printf("Primary elected and all members ready in replica set %s (replica set connection verified)", replSetName)
					return nil
				}

				// Only log error if we've exhausted all verification attempts
				// This allows the outer loop to continue and retry
				if i >= maxRetries-5 {
					// We're getting close to timeout, log the error
					log.Printf("Replica set connection not ready yet (attempt %d/%d): %v", i+1, maxRetries, lastError)
				}
				// Continue outer loop to retry - don't fail yet
			}

			if !hasPrimary {
				log.Printf("Waiting for primary election... (attempt %d/%d)", i+1, maxRetries)
			} else if !allValid {
				log.Printf("Waiting for all members to be ready... (attempt %d/%d)", i+1, maxRetries)
			} else if !allReady {
				log.Printf("Waiting for all data members to be ready (PRIMARY/SECONDARY)... (attempt %d/%d)", i+1, maxRetries)
			}
		}
	}

	return fmt.Errorf("timeout waiting for primary to be elected in replica set %s", replSetName)
}

// configureShardedCluster configures the sharded cluster by adding shards
func (l *NativeLauncher) configureShardedCluster() error {
	log.Println("Configuring sharded cluster...")

	// Connect to mongos
	mongosHosts := l.GetMongosHosts()
	if len(mongosHosts) == 0 {
		return fmt.Errorf("no mongos instances available")
	}

	connStr := fmt.Sprintf("mongodb://%s", mongosHosts[0])
	if l.config.Auth {
		username := l.config.Username
		password := l.config.Password
		if username == "" {
			username = "admin"
		}
		if password == "" {
			password = "admin"
		}
		connStr = fmt.Sprintf("mongodb://%s:%s@%s/admin", username, password, mongosHosts[0])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	if err != nil {
		return fmt.Errorf("failed to connect to mongos: %w", err)
	}
	defer client.Disconnect(ctx)

	// Add each shard
	for shard := 1; shard <= l.config.Shards; shard++ {
		replSetName := fmt.Sprintf("rs%d", shard)
		hosts := l.getShardHosts(shard)
		shardStr := fmt.Sprintf("%s/%s", replSetName, strings.Join(hosts, ","))

		// Check if shard already exists
		var shards bson.M
		err = client.Database("admin").RunCommand(ctx, bson.M{"listShards": 1}).Decode(&shards)
		if err == nil {
			if shardList, ok := shards["shards"].(bson.A); ok {
				for _, s := range shardList {
					if sMap, ok := s.(bson.M); ok {
						if id, ok := sMap["_id"].(string); ok && id == replSetName {
							log.Printf("Shard %s already added", replSetName)
							continue
						}
					}
				}
			}
		}

		// Add shard
		addShardCmd := bson.M{
			"addShard": shardStr,
		}

		err = client.Database("admin").RunCommand(ctx, addShardCmd).Err()
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				log.Printf("Shard %s already added", replSetName)
				continue
			}
			return fmt.Errorf("failed to add shard %s: %w", replSetName, err)
		}

		log.Printf("Added shard %s", replSetName)
	}

	return nil
}

// getConfigServerHosts returns config server host strings
func (l *NativeLauncher) getConfigServerHosts() []string {
	hosts := make([]string, 0)
	for _, proc := range l.processes {
		if proc.Type == "config" {
			hosts = append(hosts, proc.Host)
		}
	}
	return hosts
}

// getShardHosts returns host strings for a specific shard
func (l *NativeLauncher) getShardHosts(shard int) []string {
	shardName := fmt.Sprintf("shard%02d", shard)
	hosts := make([]string, 0)
	for _, proc := range l.processes {
		if proc.Type == "shard" && proc.Shard == shardName {
			hosts = append(hosts, proc.Host)
		}
	}
	return hosts
}

// getReplicaSetHosts returns replica set host strings
func (l *NativeLauncher) getReplicaSetHosts() []string {
	hosts := make([]string, 0)
	for _, proc := range l.processes {
		if proc.Type == "replica" {
			hosts = append(hosts, proc.Host)
		}
	}
	return hosts
}

// waitForStandaloneReady waits for standalone server to be ready
func (l *NativeLauncher) waitForStandaloneReady() error {
	log.Println("Waiting for standalone server to be ready...")

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	// Find standalone process
	var standaloneProc *ProcessInfo
	l.processMu.Lock()
	for _, proc := range l.processes {
		if proc.Type == "standalone" {
			standaloneProc = proc
			break
		}
	}
	l.processMu.Unlock()

	if standaloneProc == nil {
		return fmt.Errorf("no standalone process found")
	}

	log.Printf("Checking standalone server on port %d (PID: %d)...", standaloneProc.Port, standaloneProc.PID)
	// Use auth=false before user creation (MongoDB allows localhost connections without auth)
	if err := l.waitForPort(standaloneProc, ctx, false); err != nil {
		return fmt.Errorf("standalone server on port %d not ready: %w", standaloneProc.Port, err)
	}
	log.Printf("Standalone server on port %d is ready", standaloneProc.Port)

	return nil
}

// createAdminUser creates an admin user on a standalone MongoDB instance
// When --auth is enabled, MongoDB allows localhost connections without auth to create the first user
func (l *NativeLauncher) createAdminUser(host string) error {
	log.Printf("Creating admin user on %s...", host)

	username := l.config.Username
	password := l.config.Password
	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "admin"
	}

	// Connect WITHOUT auth (MongoDB allows this from localhost when --auth is enabled)
	connStr := fmt.Sprintf("mongodb://%s", host)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr).SetDirect(true))
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Disconnect(ctx)

	// Check if user already exists
	adminDB := client.Database("admin")
	var result bson.M
	err = adminDB.RunCommand(ctx, bson.M{"usersInfo": username}).Decode(&result)
	if err == nil {
		if users, ok := result["users"].(bson.A); ok && len(users) > 0 {
			log.Printf("Admin user %s already exists on %s", username, host)
			return nil
		}
	}

	// Create admin user with root role
	// Use bson.D (ordered document) for createUser command to avoid "multi-key map" error
	createUserCmd := bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{
			bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}},
		}},
	}

	var createUserResult bson.M
	err = adminDB.RunCommand(ctx, createUserCmd).Decode(&createUserResult)
	if err != nil {
		return fmt.Errorf("failed to create admin user: %w", err)
	}

	log.Printf("Created admin user %s on %s", username, host)
	return nil
}

// createAdminUserOnReplicaSet creates an admin user on the primary of a replica set
func (l *NativeLauncher) createAdminUserOnReplicaSet(replSetName string, hosts []string) error {
	if len(hosts) == 0 {
		return fmt.Errorf("no hosts provided for replica set %s", replSetName)
	}

	log.Printf("Creating admin user on replica set %s...", replSetName)

	// When --auth is enabled, we need to connect directly to hosts (not replica set)
	// to find the primary, because replica set connection requires authentication
	// MongoDB allows localhost connections without auth to create the first user
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Try connecting directly to each host to find the primary
	maxRetries := 10
	var primaryHost string
	var primaryClient *mongo.Client

	for i := 0; i < maxRetries; i++ {
		// Wait a bit for primary election
		time.Sleep(2 * time.Second)

		// Try each host directly to find the primary
		for _, host := range hosts {
			// Connect directly to this host (not replica set) - MongoDB allows localhost without auth
			connStr := fmt.Sprintf("mongodb://%s", host)
			clientOpts := options.Client().
				ApplyURI(connStr).
				SetDirect(true). // Connect directly, don't use replica set connection
				SetServerSelectionTimeout(2 * time.Second).
				SetConnectTimeout(2 * time.Second)

			client, err := mongo.Connect(ctx, clientOpts)
			if err != nil {
				continue // Try next host
			}

			// Try to get replica set status (this works on localhost without auth)
			var status bson.M
			err = client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&status)
			if err != nil {
				client.Disconnect(ctx)
				continue // Try next host
			}

			// Check if this host is the primary
			if members, ok := status["members"].(bson.A); ok {
				for _, m := range members {
					if member, ok := m.(bson.M); ok {
						if stateStr, ok := member["stateStr"].(string); ok && stateStr == "PRIMARY" {
							if name, ok := member["name"].(string); ok && name == host {
								primaryHost = host
								primaryClient = client
								break
							}
						}
					}
				}
			}

			if primaryHost != "" {
				break
			}

			client.Disconnect(ctx)
		}

		if primaryHost != "" {
			log.Printf("Found primary: %s", primaryHost)
			break
		}

		log.Printf("Primary not yet elected, retrying... (attempt %d/%d)", i+1, maxRetries)
	}

	if primaryHost == "" {
		return fmt.Errorf("failed to find primary in replica set %s after %d attempts", replSetName, maxRetries)
	}

	// Use the client connected to the primary
	client := primaryClient
	defer client.Disconnect(ctx)

	// Verify we can ping the primary
	err := client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to ping primary: %w", err)
	}

	// Double-check that we're still connected to the primary before creating user
	var status bson.M
	err = client.Database("admin").RunCommand(ctx, bson.M{"replSetGetStatus": 1}).Decode(&status)
	if err != nil {
		return fmt.Errorf("failed to verify primary status: %w", err)
	}

	// Verify this host is still the primary
	isPrimary := false
	if members, ok := status["members"].(bson.A); ok {
		for _, m := range members {
			if member, ok := m.(bson.M); ok {
				if stateStr, ok := member["stateStr"].(string); ok && stateStr == "PRIMARY" {
					if name, ok := member["name"].(string); ok && name == primaryHost {
						isPrimary = true
						break
					}
				}
			}
		}
	}

	if !isPrimary {
		return fmt.Errorf("host %s is no longer the primary", primaryHost)
	}

	username := l.config.Username
	password := l.config.Password
	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "admin"
	}

	// Retry creating the user multiple times in case of NotMaster errors
	maxCreateRetries := 5
	var createErr error

	for attempt := 1; attempt <= maxCreateRetries; attempt++ {
		// Check if user already exists
		adminDB := client.Database("admin")
		var result bson.M
		err = adminDB.RunCommand(ctx, bson.M{"usersInfo": username}).Decode(&result)
		if err == nil {
			if users, ok := result["users"].(bson.A); ok && len(users) > 0 {
				log.Printf("Admin user %s already exists on replica set %s", username, replSetName)
				return nil
			}
		}

		// Verify we're still on the primary before creating user
		var isMasterResult bson.M
		err = adminDB.RunCommand(ctx, bson.M{"isMaster": 1}).Decode(&isMasterResult)
		if err != nil {
			createErr = fmt.Errorf("failed to check isMaster: %w", err)
			if attempt < maxCreateRetries {
				log.Printf("Warning: failed to check isMaster (attempt %d/%d): %v, retrying...", attempt, maxCreateRetries, err)
				time.Sleep(1 * time.Second)
				// Reconnect to primary
				client.Disconnect(ctx)
				connStr := fmt.Sprintf("mongodb://%s", primaryHost)
				clientOpts := options.Client().
					ApplyURI(connStr).
					SetDirect(true).
					SetServerSelectionTimeout(2 * time.Second).
					SetConnectTimeout(2 * time.Second)
				client, err = mongo.Connect(ctx, clientOpts)
				if err != nil {
					createErr = fmt.Errorf("failed to reconnect to primary: %w", err)
					continue
				}
				adminDB = client.Database("admin")
				continue
			}
			return createErr
		}

		// Verify we're on the primary
		if isMaster, ok := isMasterResult["ismaster"].(bool); !ok || !isMaster {
			createErr = fmt.Errorf("not connected to primary (ismaster: %v)", isMaster)
			if attempt < maxCreateRetries {
				log.Printf("Warning: not on primary (attempt %d/%d), reconnecting...", attempt, maxCreateRetries)
				time.Sleep(1 * time.Second)
				// Reconnect to primary
				client.Disconnect(ctx)
				connStr := fmt.Sprintf("mongodb://%s", primaryHost)
				clientOpts := options.Client().
					ApplyURI(connStr).
					SetDirect(true).
					SetServerSelectionTimeout(2 * time.Second).
					SetConnectTimeout(2 * time.Second)
				client, err = mongo.Connect(ctx, clientOpts)
				if err != nil {
					createErr = fmt.Errorf("failed to reconnect to primary: %w", err)
					continue
				}
				adminDB = client.Database("admin")
				continue
			}
			return createErr
		}

		// Create admin user with root role
		// Use bson.D (ordered document) for createUser command to avoid "multi-key map" error
		createUserCmd := bson.D{
			{Key: "createUser", Value: username},
			{Key: "pwd", Value: password},
			{Key: "roles", Value: bson.A{
				bson.D{{Key: "role", Value: "root"}, {Key: "db", Value: "admin"}},
			}},
		}

		var createUserResult bson.M
		err = adminDB.RunCommand(ctx, createUserCmd).Decode(&createUserResult)
		if err != nil {
			createErr = err
			// Check if it's a NotMaster error or similar - retry these
			errStr := err.Error()
			if strings.Contains(errStr, "NotMaster") || strings.Contains(errStr, "not master") ||
				strings.Contains(errStr, "not primary") || strings.Contains(errStr, "NotPrimary") {
				if attempt < maxCreateRetries {
					log.Printf("Warning: failed to create user (attempt %d/%d): %v, retrying...", attempt, maxCreateRetries, err)
					time.Sleep(1 * time.Second)
					// Reconnect to primary
					client.Disconnect(ctx)
					connStr := fmt.Sprintf("mongodb://%s", primaryHost)
					clientOpts := options.Client().
						ApplyURI(connStr).
						SetDirect(true).
						SetServerSelectionTimeout(2 * time.Second).
						SetConnectTimeout(2 * time.Second)
					client, err = mongo.Connect(ctx, clientOpts)
					if err != nil {
						createErr = fmt.Errorf("failed to reconnect to primary: %w", err)
						continue
					}
					adminDB = client.Database("admin")
					continue
				}
			}
			// For other errors, don't retry
			return fmt.Errorf("failed to create admin user: %w", err)
		}

		// Success!
		createErr = nil
		break
	}

	if createErr != nil {
		return fmt.Errorf("failed to create admin user after %d attempts: %w", maxCreateRetries, createErr)
	}

	log.Printf("Created admin user %s on replica set %s", username, replSetName)

	// Now that the user is created, verify by reconnecting using replica set connection with auth
	// This allows auto-selecting the primary and confirms authentication works
	replicaSetStr := strings.Join(hosts, ",")
	authConnStr := fmt.Sprintf("mongodb://%s:%s@%s/?replicaSet=%s&authSource=admin", username, password, replicaSetStr, replSetName)

	// Create a new context for the verification connection
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer verifyCancel()

	// Connect using replica set connection with authentication (auto-selects primary)
	verifyClientOpts := options.Client().
		ApplyURI(authConnStr).
		SetDirect(false). // Use replica set connection to auto-select primary
		SetServerSelectionTimeout(5 * time.Second).
		SetConnectTimeout(5 * time.Second)

	verifyClient, err := mongo.Connect(verifyCtx, verifyClientOpts)
	if err != nil {
		log.Printf("Warning: failed to verify user creation with replica set connection: %v", err)
		// Don't fail - user was created, just verification failed
		return nil
	}
	defer verifyClient.Disconnect(verifyCtx)

	// Verify we can authenticate and connect
	err = verifyClient.Ping(verifyCtx, nil)
	if err != nil {
		log.Printf("Warning: failed to ping with authenticated replica set connection: %v", err)
		// Don't fail - user was created, just verification failed
		return nil
	}

	log.Printf("Verified admin user %s on replica set %s using replica set connection", username, replSetName)
	return nil
}

// createAdminUsers creates admin users on all replica sets in a sharded cluster
func (l *NativeLauncher) createAdminUsers() error {
	log.Println("Creating admin users on all replica sets...")

	// Create user on config server replica set
	if l.config.ConfigServers > 0 {
		configHosts := l.getConfigServerHosts()
		if len(configHosts) > 0 {
			if err := l.createAdminUserOnReplicaSet("configRepl", configHosts); err != nil {
				return fmt.Errorf("failed to create admin user on config server: %w", err)
			}
		}
	}

	// Create user on each shard replica set
	for shard := 1; shard <= l.config.Shards; shard++ {
		replSetName := fmt.Sprintf("rs%d", shard)
		hosts := l.getShardHosts(shard)
		if len(hosts) > 0 {
			if err := l.createAdminUserOnReplicaSet(replSetName, hosts); err != nil {
				return fmt.Errorf("failed to create admin user on shard %d: %w", shard, err)
			}
		}
	}

	return nil
}

// addProcess adds a process to the tracking list and saves metadata immediately
// The lock is held during the entire operation to ensure only one process can modify at a time
func (l *NativeLauncher) addProcess(proc *ProcessInfo) {
	l.processMu.Lock()
	defer l.processMu.Unlock()

	l.processes = append(l.processes, proc)

	// Save metadata immediately after adding process (lock already held)
	// This ensures processes.json is updated as soon as each node starts
	if err := l.saveProcessMetadataUnlocked(); err != nil {
		log.Printf("Warning: failed to save process metadata after adding process %d: %v", proc.PID, err)
	}
}

// Start starts all MongoDB processes from saved metadata
func (l *NativeLauncher) Start() error {
	log.Println("Starting MongoDB cluster from saved metadata...")

	// Load processes from metadata
	if err := l.loadProcessMetadata(); err != nil {
		return fmt.Errorf("failed to load process metadata: %w", err)
	}

	l.processMu.Lock()
	processes := make([]*ProcessInfo, len(l.processes))
	copy(processes, l.processes)
	l.processMu.Unlock()

	if len(processes) == 0 {
		return fmt.Errorf("no processes found in metadata file: %s", l.metadataFile)
	}

	// Find config servers for mongos processes
	configHosts := make([]string, 0)
	for _, proc := range processes {
		if proc.Type == "config" || (proc.Type == "mongod" && proc.ReplSet == "configRepl") {
			configHosts = append(configHosts, proc.Host)
		}
	}
	configStr := strings.Join(configHosts, ",")

	// Start all processes
	for _, proc := range processes {
		var newProc *ProcessInfo
		var err error

		if proc.Type == "mongos" {
			// Restart mongos
			newProc, err = l.restartMongos(proc, configStr)
		} else {
			// Restart mongod
			newProc, err = l.restartMongod(proc)
		}

		if err != nil {
			return fmt.Errorf("failed to restart process on port %d: %w", proc.Port, err)
		}

		// Update process info with new PID
		l.processMu.Lock()
		for i, p := range l.processes {
			if p.Port == proc.Port && p.Type == proc.Type {
				l.processes[i] = newProc
				break
			}
		}
		l.processMu.Unlock()

		log.Printf("Restarted %s on port %d (new PID: %d)", proc.Type, proc.Port, newProc.PID)
	}

	// Save updated metadata with new PIDs
	if err := l.saveProcessMetadata(); err != nil {
		log.Printf("Warning: failed to save process metadata: %v", err)
	}

	log.Println("All processes started successfully")
	return nil
}

// restartMongod restarts a mongod process from saved ProcessInfo
func (l *NativeLauncher) restartMongod(proc *ProcessInfo) (*ProcessInfo, error) {
	// Determine process type
	isConfig := proc.Type == "config" || proc.ReplSet == "configRepl"
	procType := "replica"
	if isConfig {
		procType = "config"
	} else if proc.Shard != "" {
		procType = "shard"
	} else if proc.ReplSet != "" {
		procType = "replica"
	} else {
		procType = "standalone"
	}

	return l.startMongod(proc.Port, proc.DataDir, proc.LogFile, procType, proc.ReplSet, isConfig)
}

// restartMongos restarts a mongos process from saved ProcessInfo
func (l *NativeLauncher) restartMongos(proc *ProcessInfo, configStr string) (*ProcessInfo, error) {
	mongosPath := filepath.Join(l.config.BinPath, "mongos")
	if runtime.GOOS == "windows" {
		mongosPath += ".exe"
	}

	// Ensure log file path is absolute
	absLogFile, err := filepath.Abs(proc.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path for log file: %w", err)
	}

	// Ensure log directory exists
	logDir := filepath.Dir(absLogFile)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory %s: %w", logDir, err)
	}

	// Create empty log file if it doesn't exist
	if _, err := os.Stat(absLogFile); os.IsNotExist(err) {
		if f, err := os.Create(absLogFile); err != nil {
			return nil, fmt.Errorf("failed to create log file %s: %w", absLogFile, err)
		} else {
			f.Close()
		}
	}

	args := []string{
		"--port", strconv.Itoa(proc.Port),
		"--configdb", fmt.Sprintf("configRepl/%s", configStr),
		"--logpath", absLogFile,
		"--logappend",
		"--bind_ip", "localhost",
	}

	if l.config.Auth && l.keyFile != "" {
		args = append(args, "--keyFile", l.keyFile)
	}

	cmd := exec.Command(mongosPath, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	log.Printf("Restarting mongos: %s %s", mongosPath, strings.Join(args, " "))

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start mongos: %w", err)
	}

	// Wait a bit for process to start
	waitTime := 500 * time.Millisecond
	if l.config.Auth {
		waitTime = 3 * time.Second
	}
	time.Sleep(waitTime)

	// Check if process is still running
	if cmd.Process == nil {
		return nil, fmt.Errorf("mongos process failed to start")
	}

	return &ProcessInfo{
		Type:    "mongos",
		Port:    proc.Port,
		PID:     cmd.Process.Pid,
		DataDir: "", // mongos doesn't have a data directory
		LogFile: absLogFile,
		ReplSet: "",
		Shard:   "",
		Host:    fmt.Sprintf("localhost:%d", proc.Port),
	}, nil
}

// Stop stops all MongoDB processes gracefully in parallel
func (l *NativeLauncher) Stop() error {
	log.Println("Stopping MongoDB cluster...")

	// Load processes from metadata if needed
	if len(l.processes) == 0 {
		if err := l.loadProcessMetadata(); err != nil {
			log.Printf("Warning: failed to load process metadata: %v", err)
		}
	}

	l.processMu.Lock()
	processes := make([]*ProcessInfo, len(l.processes))
	copy(processes, l.processes)
	l.processMu.Unlock()

	// Stop all processes in parallel
	var wg sync.WaitGroup
	for _, proc := range processes {
		wg.Add(1)
		go func(p *ProcessInfo) {
			defer wg.Done()
			if err := l.stopProcess(p.PID); err != nil {
				log.Printf("Warning: failed to stop process %d (port %d): %v", p.PID, p.Port, err)
			} else {
				log.Printf("Stopped process %d (port %d)", p.PID, p.Port)
			}
		}(proc)
	}

	// Wait for all processes to stop
	wg.Wait()

	l.processMu.Lock()
	l.processes = nil
	l.processMu.Unlock()

	return nil
}

// Reset stops all MongoDB processes and removes the data directory
func (l *NativeLauncher) Reset() error {
	log.Println("Resetting MongoDB cluster...")

	// First, stop all processes gracefully
	if err := l.Stop(); err != nil {
		log.Printf("Warning: failed to stop processes gracefully: %v", err)
		// Continue anyway - we'll try to remove the data directory
	}

	// Remove the data directory
	if l.dataDir != "" {
		absDataDir, err := filepath.Abs(l.dataDir)
		if err != nil {
			return fmt.Errorf("failed to get absolute path for data directory: %w", err)
		}

		log.Printf("Removing data directory: %s", absDataDir)
		if err := os.RemoveAll(absDataDir); err != nil {
			return fmt.Errorf("failed to remove data directory: %w", err)
		}
		log.Printf("Data directory removed successfully")
	} else {
		log.Println("No data directory specified, skipping removal")
	}

	return nil
}

// Kill forcefully kills all MongoDB processes
func (l *NativeLauncher) Kill() error {
	log.Println("Killing MongoDB cluster...")

	// Load processes from metadata if needed
	if len(l.processes) == 0 {
		if err := l.loadProcessMetadata(); err != nil {
			log.Printf("Warning: failed to load process metadata: %v", err)
		}
	}

	l.processMu.Lock()
	defer l.processMu.Unlock()

	for _, proc := range l.processes {
		if err := l.killProcess(proc.PID); err != nil {
			log.Printf("Warning: failed to kill process %d: %v", proc.PID, err)
		} else {
			log.Printf("Killed process %d (port %d)", proc.PID, proc.Port)
		}
	}

	l.processes = nil
	return nil
}

// KillAll kills all mongod and mongos processes on the system (not just those managed by this launcher)
func (l *NativeLauncher) KillAll() error {
	log.Println("Killing all mongod and mongos processes on the system...")

	processNames := []string{"mongod", "mongos"}

	if runtime.GOOS == "windows" {
		// On Windows, use taskkill to kill all processes by name
		for _, procName := range processNames {
			cmd := exec.Command("taskkill", "/F", "/IM", fmt.Sprintf("%s.exe", procName))
			if err := cmd.Run(); err != nil {
				// Process might not exist, that's okay
				log.Printf("No %s processes found or already killed", procName)
			} else {
				log.Printf("Killed all %s processes", procName)
			}
		}
		return nil
	}

	// On Unix-like systems, kill by process group ID (PGID)
	// Find all PIDs for mongod and mongos processes
	var allPIDs []int
	for _, procName := range processNames {
		cmd := exec.Command("pgrep", "-f", procName)
		output, err := cmd.Output()
		if err != nil {
			// No processes found
			continue
		}

		// Parse PIDs from output
		lines := strings.Split(strings.TrimSpace(string(output)), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if pid, err := strconv.Atoi(line); err == nil {
				allPIDs = append(allPIDs, pid)
			}
		}
	}

	if len(allPIDs) == 0 {
		log.Println("No mongod or mongos processes found")
		return nil
	}

	log.Printf("Found %d mongod/mongos process(es), checking status...", len(allPIDs))

	// Check for processes in uninterruptible sleep (UE state)
	var uePIDs []int
	for _, pid := range allPIDs {
		// Get process state using ps -o stat= -p <pid>
		cmd := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid))
		output, err := cmd.Output()
		if err != nil {
			// Process might be gone, skip it
			continue
		}

		state := strings.TrimSpace(string(output))
		// Check if state contains 'D' (uninterruptible sleep) or 'U' (uninterruptible wait)
		// UE typically means uninterruptible sleep
		if strings.Contains(state, "D") || strings.Contains(state, "U") {
			uePIDs = append(uePIDs, pid)
		}
	}

	if len(uePIDs) > 0 {
		log.Printf("Warning: Found %d process(es) in uninterruptible sleep (UE state): %v", len(uePIDs), uePIDs)
		log.Println("")
		log.Println("These processes cannot be killed as they are in uninterruptible sleep.")
		log.Println("This typically happens when a process is waiting on I/O operations that cannot be interrupted.")
		log.Println("")
		log.Println("Recommendation: Restart your system to clear these processes.")
		log.Println("Alternatively, wait for the I/O operations to complete.")
		log.Println("")
	}

	// Get unique process group IDs
	pgidMap := make(map[int]bool)
	for _, pid := range allPIDs {
		// Skip processes in uninterruptible sleep
		isUE := false
		for _, uePID := range uePIDs {
			if pid == uePID {
				isUE = true
				break
			}
		}
		if isUE {
			continue
		}

		// Get process group ID for this PID
		cmd := exec.Command("ps", "-o", "pgid=", "-p", strconv.Itoa(pid))
		output, err := cmd.Output()
		if err != nil {
			// Process might be gone, skip it
			continue
		}

		pgidStr := strings.TrimSpace(string(output))
		if pgid, err := strconv.Atoi(pgidStr); err == nil {
			pgidMap[pgid] = true
		}
	}

	if len(pgidMap) == 0 {
		if len(uePIDs) > 0 {
			log.Println("No killable process groups found (all processes are in uninterruptible sleep)")
		} else {
			log.Println("No process groups found")
		}
		return nil
	}

	// Kill each process group using kill -9 -<PGID> (negative PGID means kill the group)
	killedCount := 0
	for pgid := range pgidMap {
		// Use syscall.Kill with negative PGID to kill the entire process group
		if err := syscall.Kill(-pgid, syscall.SIGKILL); err == nil {
			killedCount++
			log.Printf("Killed process group %d", pgid)
		} else {
			log.Printf("Warning: failed to kill process group %d: %v", pgid, err)
		}
	}

	// Wait a moment for processes to die
	time.Sleep(500 * time.Millisecond)

	log.Printf("Successfully killed %d process group(s)", killedCount)
	return nil
}

// stopProcess stops a process gracefully
func (l *NativeLauncher) stopProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		// Process might already be dead
		return nil
	}

	// Try graceful shutdown (SIGTERM on Unix, or use shutdown command)
	if runtime.GOOS == "windows" {
		// On Windows, use taskkill for graceful shutdown
		cmd := exec.Command("taskkill", "/PID", strconv.Itoa(pid), "/T")
		if err := cmd.Run(); err != nil {
			// Process might already be dead
			return nil
		}
	} else {
		// On Unix, send SIGTERM
		if err := process.Signal(os.Interrupt); err != nil {
			// Process might already be dead
			return nil
		}

		// Wait a bit for graceful shutdown
		time.Sleep(2 * time.Second)

		// Check if still running by sending signal 0
		if err := process.Signal(os.Signal(nil)); err == nil {
			// Force kill if still running
			return process.Kill()
		}
	}

	return nil
}

// killProcess forcefully kills a process
func (l *NativeLauncher) killProcess(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("process %d not found: %w", pid, err)
	}

	return process.Kill()
}

// List returns information about running processes
func (l *NativeLauncher) List() (string, error) {
	// Load processes from metadata
	if err := l.loadProcessMetadata(); err != nil {
		return "", fmt.Errorf("failed to load process metadata: %w", err)
	}

	var sb strings.Builder
	sb.WriteString("MongoDB Cluster Processes:\n")
	sb.WriteString("==========================\n\n")

	l.processMu.Lock()
	defer l.processMu.Unlock()

	for _, proc := range l.processes {
		// Check if process is still running
		running := "unknown"
		process, err := os.FindProcess(proc.PID)
		if err != nil {
			running = "not found"
		} else {
			// Try to check if process is running
			if runtime.GOOS == "windows" {
				// On Windows, check using tasklist
				cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("PID eq %d", proc.PID))
				if err := cmd.Run(); err == nil {
					running = "running"
				} else {
					running = "stopped"
				}
			} else {
				// On Unix, send signal 0
				if err := process.Signal(os.Signal(nil)); err == nil {
					running = "running"
				} else {
					running = "stopped"
				}
			}
		}

		sb.WriteString(fmt.Sprintf("Type: %s\n", proc.Type))
		sb.WriteString(fmt.Sprintf("  Port: %d\n", proc.Port))
		sb.WriteString(fmt.Sprintf("  PID: %d\n", proc.PID))
		sb.WriteString(fmt.Sprintf("  Status: %s\n", running))
		if proc.ReplSet != "" {
			sb.WriteString(fmt.Sprintf("  Replica Set: %s\n", proc.ReplSet))
		}
		if proc.Shard != "" {
			sb.WriteString(fmt.Sprintf("  Shard: %s\n", proc.Shard))
		}
		sb.WriteString("\n")
	}

	return sb.String(), nil
}

// saveProcessMetadata saves process information to disk
// This function acquires the lock itself
func (l *NativeLauncher) saveProcessMetadata() error {
	l.processMu.Lock()
	defer l.processMu.Unlock()
	return l.saveProcessMetadataUnlocked()
}

// saveProcessMetadataUnlocked saves process information to disk
// This function assumes the lock is already held (for use within locked sections)
func (l *NativeLauncher) saveProcessMetadataUnlocked() error {
	metadata := ClusterMetadata{
		Processes:    l.processes,
		BinPath:      l.config.BinPath,
		MongoVersion: l.config.MongoVersion,
		Auth:         l.config.Auth,
		KeyFile:      l.keyFile,
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal process metadata: %w", err)
	}

	if err := os.WriteFile(l.metadataFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write process metadata: %w", err)
	}

	return nil
}

// LoadMetadata loads cluster metadata from disk (public method for CLI use)
func (l *NativeLauncher) LoadMetadata() error {
	return l.loadProcessMetadata()
}

// loadProcessMetadata loads process information from disk
func (l *NativeLauncher) loadProcessMetadata() error {
	data, err := os.ReadFile(l.metadataFile)
	if err != nil {
		return fmt.Errorf("failed to read process metadata: %w", err)
	}

	// Try to unmarshal as ClusterMetadata first (new format)
	var metadata ClusterMetadata
	if err := json.Unmarshal(data, &metadata); err == nil {
		// New format with metadata
		l.processMu.Lock()
		defer l.processMu.Unlock()
		l.processes = metadata.Processes

		// Update config with saved values if not already set
		if metadata.BinPath != "" && l.config.BinPath == "" {
			l.config.BinPath = metadata.BinPath
		}
		if metadata.MongoVersion != "" && l.config.MongoVersion == "" {
			l.config.MongoVersion = metadata.MongoVersion
		}
		if metadata.Auth {
			l.config.Auth = metadata.Auth
		}
		if metadata.KeyFile != "" && l.keyFile == "" {
			l.keyFile = metadata.KeyFile
		}
		return nil
	}

	// Fall back to old format (just processes array) for backward compatibility
	var processes []*ProcessInfo
	if err := json.Unmarshal(data, &processes); err != nil {
		return fmt.Errorf("failed to unmarshal process metadata: %w", err)
	}

	l.processMu.Lock()
	defer l.processMu.Unlock()
	l.processes = processes

	return nil
}

// getShellCommand returns the appropriate shell command path and arguments based on MongoDB version
// For MongoDB 6.0+, uses mongosh; for older versions, uses mongo
func (l *NativeLauncher) getShellCommand(connectionString string) (string, []string, error) {
	var shellPath string
	var shellArgs []string

	// Determine which shell to use based on version
	useMongosh := false
	if l.config.MongoVersion != "" {
		// Normalize version for semver comparison
		version := l.config.MongoVersion
		if !strings.HasPrefix(version, "v") {
			version = "v" + version
		}
		// Ensure we have at least major.minor.patch format
		parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
		if len(parts) == 2 {
			version = "v" + strings.Join(parts, ".") + ".0"
		}
		// Use mongosh for MongoDB 6.0+
		if semver.Compare(version, "v6.0.0") >= 0 {
			useMongosh = true
		}
	} else {
		// If version not specified, default to mongosh (safer for newer versions)
		useMongosh = true
	}

	// Try mongosh first (for MongoDB 6.0+)
	if useMongosh {
		mongoshPath := filepath.Join(l.config.BinPath, "mongosh")
		if runtime.GOOS == "windows" {
			mongoshPath += ".exe"
		}
		if _, err := os.Stat(mongoshPath); err == nil {
			shellPath = mongoshPath
			shellArgs = []string{connectionString}
			return shellPath, shellArgs, nil
		}
		// If mongosh not found but version requires it, return error
		if l.config.MongoVersion != "" {
			return "", nil, fmt.Errorf("mongosh not found in %s (required for MongoDB %s+)", l.config.BinPath, l.config.MongoVersion)
		}
		// If version not specified and mongosh not found, fall back to mongo
	}

	// Fall back to mongo shell (for MongoDB < 6.0 or if mongosh not found)
	mongoPath := filepath.Join(l.config.BinPath, "mongo")
	if runtime.GOOS == "windows" {
		mongoPath += ".exe"
	}
	if _, err := os.Stat(mongoPath); err == nil {
		shellPath = mongoPath
		shellArgs = []string{connectionString}
		return shellPath, shellArgs, nil
	}

	return "", nil, fmt.Errorf("neither mongosh nor mongo shell found in %s", l.config.BinPath)
}

// GetMongosHosts returns the mongos connection strings
func (l *NativeLauncher) GetMongosHosts() []string {
	hosts := make([]string, 0)
	for _, proc := range l.processes {
		if proc.Type == "mongos" {
			hosts = append(hosts, proc.Host)
		}
	}
	return hosts
}

// GetConnectionString returns a MongoDB connection string for the cluster
func (l *NativeLauncher) GetConnectionString() string {
	hosts := l.GetMongosHosts()
	if len(hosts) == 0 {
		// Fallback to first available process
		if len(l.processes) > 0 {
			hosts = []string{l.processes[0].Host}
		} else {
			return ""
		}
	}

	connStr := "mongodb://"

	if l.config.Auth {
		username := l.config.Username
		password := l.config.Password
		if username == "" {
			username = "admin"
		}
		if password == "" {
			password = "admin"
		}
		connStr += fmt.Sprintf("%s:%s@", username, password)
	}

	connStr += strings.Join(hosts, ",")

	if l.config.Auth {
		connStr += "/admin"
	}

	return connStr
}
