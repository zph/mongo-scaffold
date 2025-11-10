// Package mongocluster provides a programmatic interface for managing MongoDB test clusters.
// It allows you to start and stop MongoDB clusters programmatically, making it easy to
// integrate MongoDB cluster management into your Go applications.
//
// # Basic Usage
//
// Start a basic cluster with default settings:
//
//	cfg := mongocluster.StartConfig{
//		MongoVersion: mongocluster.DefaultMongoVersion,
//		Shards:       mongocluster.DefaultShards,
//		ReplicaNodes: mongocluster.DefaultReplicaNodes,
//	}
//
//	result, err := mongocluster.Start(cfg, nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Use the cluster
//	connStr := result.Cluster.ConnectionString()
//	fmt.Println("Connection string:", connStr)
//
//	// Wait for user to stop (foreground mode)
//	if err := mongocluster.WaitForStop(result.Cluster, nil); err != nil {
//		log.Fatal(err)
//	}
//
// # Starting with Custom Configuration
//
// Start a cluster with custom settings:
//
//	cfg := mongocluster.StartConfig{
//		MongoVersion:   "5.0",
//		Shards:        3,
//		ReplicaNodes:  5,
//		ConfigServers: 3,
//		MongosCount:   2,
//		Auth:          true,
//		Username:     "admin",
//		Password:     "secret",
//		Timeout:       90 * time.Second,      // Health check timeout
//		StartupTimeout: 5 * time.Minute,      // Maximum time for cluster startup
//		OutputFile:    "cluster-info.json",
//		Background:    false, // false = foreground mode
//	}
//
//	result, err := mongocluster.Start(cfg, nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Starting in Background Mode
//
// Start a cluster and exit immediately (background mode):
//
//	cfg := mongocluster.StartConfig{
//		MongoVersion: mongocluster.DefaultMongoVersion,
//		Background:  true, // Start in background
//		OutputFile:   "cluster-info.json", // Save info for later stop
//	}
//
//	result, err := mongocluster.Start(cfg, nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	// Cluster is running, program exits
//
// # Setting Startup Timeout
//
// Set a maximum time to wait for cluster startup:
//
//	cfg := mongocluster.StartConfig{
//		MongoVersion:   mongocluster.DefaultMongoVersion,
//		StartupTimeout: 5 * time.Minute, // Fail if startup takes longer than 5 minutes
//		OutputFile:    "cluster-info.json",
//	}
//
//	result, err := mongocluster.Start(cfg, nil)
//	if err != nil {
//		log.Fatal(err) // Will fail if startup exceeds 5 minutes
//	}
//
// # Stopping a Cluster
//
// Stop a cluster using the cluster info file:
//
//	err := mongocluster.Stop("cluster-info.json", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Reading Cluster Information
//
// Read cluster information from a saved file:
//
//	info, err := mongocluster.ReadClusterInfoFromFile("cluster-info.json")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Println("Connection string:", info.ConnectionString)
//	fmt.Println("Data directory:", info.DataDirectory)
//	fmt.Println("Mongos hosts:", info.MongosHosts)
//
// # Using Custom Logger and Output Writer
//
// Use custom logger and output writer:
//
//	customLogger := log.New(os.Stderr, "[CLUSTER] ", log.LstdFlags)
//	customOutput := os.Stderr
//
//	opts := &mongocluster.StartOptions{
//		Logger:      customLogger,
//		OutputWriter: customOutput,
//		PortMin:     30000,
//		PortMax:     40000,
//	}
//
//	result, err := mongocluster.Start(cfg, opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// # Complete Example
//
// A complete example that starts a cluster, uses it, and stops it:
//
//	package main
//
//	import (
//		"log"
//		"time"
//
//		"github.com/zph/mongo-scaffold/pkg/mongocluster"
//	)
//
//	func main() {
//		// Start cluster
//		cfg := mongocluster.StartConfig{
//			MongoVersion: "3.6",
//			Shards:       2,
//			ReplicaNodes: 3,
//			OutputFile:   "my-cluster.json",
//			Background:   false,
//		}
//
//		result, err := mongocluster.Start(cfg, nil)
//		if err != nil {
//			log.Fatalf("Failed to start cluster: %v", err)
//		}
//
//		// Use the cluster
//		log.Printf("Cluster started at: %s", result.Cluster.ConnectionString())
//		log.Printf("Mongos hosts: %v", result.Cluster.MongosHosts())
//
//		// Wait for stop signal (foreground mode)
//		if err := mongocluster.WaitForStop(result.Cluster, nil); err != nil {
//			log.Fatalf("Error waiting for stop: %v", err)
//		}
//
//		// Or stop programmatically
//		// err = mongocluster.Stop("my-cluster.json", nil)
//	}
package mongocluster

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/zph/mongo-scaffold/pkg/cluster"
	"github.com/zph/mongo-scaffold/pkg/mlaunch"
	"github.com/zph/mongo-scaffold/pkg/mongo_version_manager"
	"github.com/zph/mongo-scaffold/pkg/portmanager"
	"golang.org/x/mod/semver"
)

// Defaults for cluster configuration
const (
	DefaultMongoVersion  = "3.6"
	DefaultShards        = 2
	DefaultReplicaNodes  = 3
	DefaultConfigServers = 1
	DefaultMongosCount   = 1
	// DefaultStartupTimeout is the default maximum time to wait for cluster startup
	// This is used when StartupTimeout is 0 (not set)
	DefaultStartupTimeout = 5 * time.Minute
)

// StartConfig represents the configuration for starting a cluster
type StartConfig struct {
	MongoVersion   string
	Shards         int
	ReplicaNodes   int
	ConfigServers  int
	MongosCount    int
	Auth           bool
	Username       string
	Password       string
	Timeout        time.Duration // Health check timeout
	StartupTimeout time.Duration // Maximum time to wait for cluster startup
	OutputFile     string
	FileOverwrite  bool // If true, delete existing OutputFile if it exists
	Background     bool
	KeepTempDir    bool // If true, don't delete temp directory on teardown (for debugging)
}

// StartOptions provides options for customizing the start behavior
type StartOptions struct {
	// Writer for output (defaults to os.Stdout)
	OutputWriter io.Writer
	// Logger for log messages (defaults to log.Default())
	Logger *log.Logger
	// Port range for finding available ports
	PortMin int
	PortMax int
}

// StartResult contains information about a started cluster
type StartResult struct {
	Cluster         *cluster.Cluster
	Config          cluster.Config
	ClusterInfo     *ClusterInfo
	ClusterInfoFile string
}

// Start starts a MongoDB cluster with the given configuration
func Start(cfg StartConfig, opts *StartOptions) (*StartResult, error) {
	if opts == nil {
		opts = &StartOptions{}
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	outputWriter := opts.OutputWriter
	if outputWriter == nil {
		outputWriter = os.Stdout
	}

	// Safety check: fail if cluster.json file already exists (indicates cluster already initialized)
	// Unless FileOverwrite is true, in which case delete the existing file
	if cfg.OutputFile != "" {
		if _, err := os.Stat(cfg.OutputFile); err == nil {
			if cfg.FileOverwrite {
				logger.Printf("File %s already exists and --file-overwrite is true, deleting existing file...", cfg.OutputFile)
				if err := os.Remove(cfg.OutputFile); err != nil {
					return nil, fmt.Errorf("failed to delete existing file %s: %w", cfg.OutputFile, err)
				}
				logger.Printf("Deleted existing file: %s", cfg.OutputFile)
			} else {
				return nil, fmt.Errorf("cluster already initialized: %s exists. Remove the file, use --file-overwrite, or use a different output file path", cfg.OutputFile)
			}
		}
	}

	portMin := opts.PortMin
	if portMin == 0 {
		portMin = 30000
	}
	portMax := opts.PortMax
	if portMax == 0 {
		portMax = 40000
	}

	// Apply default startup timeout if not set (0 is sentinel value)
	startupTimeout := cfg.StartupTimeout
	if startupTimeout == 0 {
		startupTimeout = DefaultStartupTimeout
	}

	// Create cluster configuration
	config := cluster.Config{
		MongoVersion:       cfg.MongoVersion,
		Shards:             cfg.Shards,
		ReplicaNodes:       cfg.ReplicaNodes,
		ConfigServers:      cfg.ConfigServers,
		MongosCount:        cfg.MongosCount,
		Auth:               cfg.Auth,
		Username:           cfg.Username,
		Password:           cfg.Password,
		HealthCheckTimeout: cfg.Timeout,
		StartupTimeout:     startupTimeout,
		KeepTempDir:        cfg.KeepTempDir,
	}

	logger.Printf("Creating MongoDB cluster with configuration:")
	logger.Printf("  Version: %s", config.MongoVersion)
	logger.Printf("  Shards: %d", config.Shards)
	logger.Printf("  Replica Nodes: %d", config.ReplicaNodes)
	logger.Printf("  Config Servers: %d", config.ConfigServers)
	logger.Printf("  Mongos Routers: %d", config.MongosCount)
	logger.Printf("  Authentication: %v", config.Auth)
	logger.Printf("  Startup Timeout: %v", config.StartupTimeout)

	// Create the cluster
	c, err := cluster.NewCluster(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster: %w", err)
	}

	// Query system for sequential available ports
	logger.Println("Querying system for sequential available ports...")
	portsNeeded := calculatePortsNeeded(cfg.Shards, cfg.ReplicaNodes, cfg.ConfigServers, cfg.MongosCount)
	availablePorts, err := portmanager.FindSequentialAvailablePorts(portsNeeded, portMin, portMax)
	if err != nil {
		return nil, fmt.Errorf("failed to find sequential available ports: %w", err)
	}
	logger.Printf("✓ Found %d sequential available ports starting at %d", len(availablePorts), availablePorts[0])

	// Start the cluster with the specific ports
	logger.Println("Starting cluster with allocated ports...")
	if err := c.StartWithPorts(availablePorts); err != nil {
		return nil, fmt.Errorf("failed to start cluster: %w", err)
	}

	// Print cluster information
	separator := strings.Repeat("=", 70)
	fmt.Fprintf(outputWriter, "\n%s\n", separator)
	fmt.Fprintf(outputWriter, "Cluster started successfully!\n")
	fmt.Fprintf(outputWriter, "%s\n", separator)
	fmt.Fprintf(outputWriter, "\nConnection String: %s\n", c.ConnectionString())
	fmt.Fprintf(outputWriter, "\nMongos Hosts:\n")
	for i, host := range c.MongosHosts() {
		fmt.Fprintf(outputWriter, "  [%d] %s\n", i+1, host)
	}
	fmt.Fprintf(outputWriter, "\nData Directory: %s\n", c.DataDir())
	fmt.Fprintf(outputWriter, "Logs Directory: %s\n", c.LogDir())
	fmt.Fprintf(outputWriter, "\n%s\n", separator)

	// Determine shell name based on version (mongosh for 6.0+, mongo for older)
	shellName := "mongo"
	if cfg.MongoVersion != "" {
		// Normalize version for comparison
		version := cfg.MongoVersion
		if !strings.HasPrefix(version, "v") {
			version = "v" + version
		}
		parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
		if len(parts) == 2 {
			version = "v" + strings.Join(parts, ".") + ".0"
		}
		// Use mongosh for MongoDB 6.0+
		if semver.Compare(version, "v6.0.0") >= 0 {
			shellName = "mongosh"
		}
	} else {
		// Default to mongosh if version not specified (safer for newer versions)
		shellName = "mongosh"
	}

	if config.Auth {
		fmt.Fprintf(outputWriter, "\nAuthentication Enabled:\n")
		fmt.Fprintf(outputWriter, "  Username: %s\n", config.Username)
		fmt.Fprintf(outputWriter, "  Password: %s\n", config.Password)
		fmt.Fprintf(outputWriter, "\nConnect with %s shell:\n", shellName)
		fmt.Fprintf(outputWriter, "  %s %s -u %s -p %s\n", shellName, c.MongosHosts()[0], config.Username, config.Password)
	} else {
		fmt.Fprintf(outputWriter, "\nConnect with %s shell:\n", shellName)
		fmt.Fprintf(outputWriter, "  %s %s\n", shellName, c.MongosHosts()[0])
	}

	fmt.Fprintf(outputWriter, "\n%s\n", separator)

	// Get MongoDB binary path for connection command
	binPath := ""
	if cfg.MongoVersion != "" {
		mongoMgr, err := mongo_version_manager.NewManager()
		if err == nil {
			if path, err := mongoMgr.GetBinPath(cfg.MongoVersion); err == nil {
				binPath = path
			}
		}
	}

	// Build cluster info
	clusterInfo := buildClusterInfo(c, config, cfg.MongoVersion, binPath)

	// Write cluster information to file if specified
	if cfg.OutputFile != "" {
		if err := WriteClusterInfoToFile(cfg.OutputFile, clusterInfo); err != nil {
			logger.Printf("Warning: failed to write cluster info to file: %v", err)
		} else {
			logger.Printf("Cluster information written to: %s", cfg.OutputFile)
		}
	}

	result := &StartResult{
		Cluster:         c,
		Config:          config,
		ClusterInfo:     clusterInfo,
		ClusterInfoFile: cfg.OutputFile,
	}

	return result, nil
}

// WaitForStop waits for user input or signals to stop the cluster
// This is used in foreground mode
func WaitForStop(c *cluster.Cluster, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}

	// Setup cleanup handler for interrupt-like signals
	// Including gentle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh,
		os.Interrupt,    // SIGINT (Ctrl+C)
		syscall.SIGTERM, // Termination signal (gentle)
		syscall.SIGHUP,  // Hangup signal (gentle)
		syscall.SIGQUIT, // Quit signal (gentle)
		syscall.SIGUSR1, // User-defined signal 1 (gentle)
		syscall.SIGUSR2, // User-defined signal 2 (gentle)
	)

	// Channel for ESC key press
	escCh := make(chan bool, 1)

	// Goroutine to handle signals
	go func() {
		sig := <-sigCh
		logger.Printf("\nReceived signal %v, tearing down cluster...", sig)
		if err := c.Teardown(); err != nil {
			logger.Printf("Error during teardown: %v", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()

	// Goroutine to handle ESC key press or Enter key
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			char, _, err := reader.ReadRune()
			if err != nil {
				// EOF or error, exit goroutine
				return
			}
			// ESC key is ASCII 27
			// Enter key is ASCII 13 (carriage return) or 10 (line feed)
			if char == 27 || char == 13 || char == 10 {
				escCh <- true
				return
			}
		}
	}()

	fmt.Println("\nCluster is running. Press ESC or Enter to stop.")
	// Block until interrupted, ESC, or Enter is pressed
	select {
	case <-escCh:
		logger.Println("\nKey pressed, tearing down cluster...")
		if err := c.Teardown(); err != nil {
			logger.Printf("Error during teardown: %v", err)
			return fmt.Errorf("teardown failed: %w", err)
		}
		return nil
	case <-sigCh:
		// Signal handler will handle this
		return nil
	}
}

// Stop stops a MongoDB cluster using the cluster info file
func Stop(clusterInfoFile string, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}

	// Read cluster info from file
	clusterInfo, err := ReadClusterInfoFromFile(clusterInfoFile)
	if err != nil {
		return fmt.Errorf("failed to read cluster info from file: %w", err)
	}

	// Extract data directory from cluster info
	dataDir := clusterInfo.DataDirectory
	if dataDir == "" {
		return fmt.Errorf("data directory not found in cluster info file")
	}

	logger.Printf("Stopping cluster from data directory: %s", dataDir)

	// Create a launcher with the data directory
	// We only need the data directory to stop the cluster
	launchConfig := mlaunch.Config{
		DataDir: dataDir,
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	// Stop the cluster
	if err := launcher.Stop(); err != nil {
		return fmt.Errorf("failed to stop cluster: %w", err)
	}

	logger.Println("Cluster stopped successfully")
	return nil
}

// calculatePortsNeeded calculates the total number of ports needed for the cluster
func calculatePortsNeeded(shards, replicaNodes, configServers, mongosCount int) int {
	// Each shard has replicaNodes mongod instances
	// Plus config servers and mongos instances
	return (shards * replicaNodes) + configServers + mongosCount
}

// buildClusterInfo builds a ClusterInfo from a cluster and config
func buildClusterInfo(c *cluster.Cluster, config cluster.Config, mongoVersion string, binPath string) *ClusterInfo {
	// Get all port information from cluster
	ports := c.Ports()
	basePort := c.BasePort()

	// Get all hosts (mongos hosts)
	hosts := c.MongosHosts()

	// Build port mapping (host -> port)
	portMapping := make(map[string]string)
	for _, host := range hosts {
		parts := strings.Split(host, ":")
		if len(parts) == 2 {
			portMapping[host] = parts[1]
		}
	}

	// Add all ports to port mapping with generic labels
	// Format: "mongos0", "mongos1", "config0", "shard0-rs0", etc.
	portIndex := 0
	// Mongos ports come first
	for i := 0; i < config.MongosCount && portIndex < len(ports); i++ {
		portMapping[fmt.Sprintf("mongos%d", i)] = fmt.Sprintf("%d", ports[portIndex])
		portIndex++
	}
	// Config server ports
	for i := 0; i < config.ConfigServers && portIndex < len(ports); i++ {
		portMapping[fmt.Sprintf("config%d", i)] = fmt.Sprintf("%d", ports[portIndex])
		portIndex++
	}
	// Shard ports
	for shard := 0; shard < config.Shards && portIndex < len(ports); shard++ {
		for node := 0; node < config.ReplicaNodes && portIndex < len(ports); node++ {
			portMapping[fmt.Sprintf("shard%d-rs%d", shard, node)] = fmt.Sprintf("%d", ports[portIndex])
			portIndex++
		}
	}

	// Build cluster info
	info := &ClusterInfo{
		ConnectionString: c.ConnectionString(),
		MongosHosts:      c.MongosHosts(),
		DataDirectory:    c.DataDir(),
		LogsDirectory:    c.LogDir(),
		BasePort:         basePort,
		Ports:            ports,
		PortMapping:      portMapping,
		Hosts:            hosts,
		Version:          config.MongoVersion,
		BinPath:          binPath,
		Topology: TopologyInfo{
			Shards:        config.Shards,
			ReplicaNodes:  config.ReplicaNodes,
			ConfigServers: config.ConfigServers,
			MongosCount:   config.MongosCount,
		},
	}

	if config.Auth {
		info.Auth = &AuthInfo{
			Enabled:  true,
			Username: config.Username,
			Password: config.Password,
		}
	}

	// Build connection command with appropriate shell binary (absolute path)
	shellPath := getShellPath(binPath, mongoVersion)
	// Use the full connection string from the cluster (includes protocol and auth if enabled)
	fullConnStr := c.ConnectionString()
	// Quote the connection string
	// For mongosh, prefix with export to disable telemetry
	if strings.Contains(shellPath, "mongosh") {
		info.ConnectionCommand = fmt.Sprintf("export MONGOSH_DISABLE_TELEMETRY=true && %s \"%s\"", shellPath, fullConnStr)
	} else {
		info.ConnectionCommand = fmt.Sprintf("%s \"%s\"", shellPath, fullConnStr)
	}

	return info
}

// getShellPath returns the absolute path to the appropriate shell binary based on MongoDB version
// Returns absolute path to "mongosh" for MongoDB 6.0+, "mongo" for older versions
// If binPath is empty, returns just the binary name
func getShellPath(binPath string, mongoVersion string) string {
	// Determine which shell to use based on version
	useMongosh := false
	if mongoVersion != "" {
		// Normalize version for comparison
		version := mongoVersion
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
		// Default to mongosh if version not specified (safer for newer versions)
		useMongosh = true
	}

	// Build absolute path if binPath is provided
	if binPath != "" {
		if useMongosh {
			shellPath := filepath.Join(binPath, "mongosh")
			if runtime.GOOS == "windows" {
				shellPath += ".exe"
			}
			return shellPath
		} else {
			shellPath := filepath.Join(binPath, "mongo")
			if runtime.GOOS == "windows" {
				shellPath += ".exe"
			}
			return shellPath
		}
	}

	// Fall back to just binary name if binPath is not available
	if useMongosh {
		return "mongosh"
	}
	return "mongo"
}
