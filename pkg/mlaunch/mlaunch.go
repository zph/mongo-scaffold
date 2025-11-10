package mlaunch

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

// Config represents the configuration for a MongoDB cluster
type Config struct {
	DataDir       string
	Shards        int
	ReplicaNodes  int
	ConfigServers int
	MongosCount   int
	BasePort      int    // Starting port (deprecated: use PortRange)
	PortRange     string // Port range in format "start-end" or "start:end" (e.g., "27017-28000")
	PortMin       int    // Minimum port (used if PortRange is not set)
	PortMax       int    // Maximum port (used if PortRange is not set, 0 = no limit)
	Auth          bool
	Username      string
	Password      string
	BinPath       string // Path to MongoDB binaries
	MongoVersion  string // MongoDB version (e.g., "3.6", "4.0", "4.4", "5.0")
}

// LauncherInterface defines the interface for MongoDB cluster launchers
type LauncherInterface interface {
	Init() error
	Start() error
	Stop() error
	Kill() error
	KillAll() error
	Reset() error
	List() (string, error)
	GetMongosHosts() []string
	GetConnectionString() string
}

// Launcher handles mlaunch operations (Python wrapper - kept for backward compatibility)
type Launcher struct {
	mlaunchPath string
	config      Config
}

// NewLauncher creates a new mlaunch wrapper
func NewLauncher(mlaunchPath string, config Config) *Launcher {
	return &Launcher{
		mlaunchPath: mlaunchPath,
		config:      config,
	}
}

// Init initializes a new MongoDB cluster
func (l *Launcher) Init() error {
	args := l.buildInitArgs()

	cmd := exec.Command(l.mlaunchPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = l.config.DataDir

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to initialize cluster: %w", err)
	}

	return nil
}

// buildInitArgs constructs the command-line arguments for mlaunch init
func (l *Launcher) buildInitArgs() []string {
	args := []string{"init"}

	// Data directory
	if l.config.DataDir != "" {
		args = append(args, "--dir", l.config.DataDir)
	}

	// Replication
	args = append(args, "--replicaset")

	// Replica nodes
	if l.config.ReplicaNodes > 0 {
		args = append(args, "--nodes", strconv.Itoa(l.config.ReplicaNodes))
	}

	// Sharding
	if l.config.Shards > 0 {
		args = append(args, "--sharded", strconv.Itoa(l.config.Shards))
	}

	// Config servers
	if l.config.ConfigServers > 0 {
		args = append(args, "--config", strconv.Itoa(l.config.ConfigServers))
	}

	// Mongos instances
	if l.config.MongosCount > 0 {
		args = append(args, "--mongos", strconv.Itoa(l.config.MongosCount))
	}

	// Port
	if l.config.BasePort > 0 {
		args = append(args, "--port", strconv.Itoa(l.config.BasePort))
	}

	// Authentication
	if l.config.Auth {
		args = append(args, "--auth")
		if l.config.Username != "" {
			args = append(args, "--username", l.config.Username)
		}
		if l.config.Password != "" {
			args = append(args, "--password", l.config.Password)
		}
	}

	// Binary path
	if l.config.BinPath != "" {
		args = append(args, "--binarypath", l.config.BinPath)
	}

	return args
}

// Stop stops the MongoDB cluster
func (l *Launcher) Stop() error {
	cmd := exec.Command(l.mlaunchPath, "stop", "--dir", l.config.DataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to stop cluster: %w", err)
	}

	return nil
}

// Start starts the MongoDB cluster from saved metadata
func (l *Launcher) Start() error {
	cmd := exec.Command(l.mlaunchPath, "start", "--dir", l.config.DataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start cluster: %w", err)
	}

	return nil
}

// Kill forcefully kills the MongoDB cluster
func (l *Launcher) Kill() error {
	cmd := exec.Command(l.mlaunchPath, "kill", "--dir", l.config.DataDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to kill cluster: %w", err)
	}

	return nil
}

// List lists the running processes
func (l *Launcher) List() (string, error) {
	cmd := exec.Command(l.mlaunchPath, "list", "--dir", l.config.DataDir)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to list processes: %w", err)
	}

	return string(output), nil
}

// GetMongosHosts returns the mongos connection strings
func (l *Launcher) GetMongosHosts() []string {
	hosts := make([]string, l.config.MongosCount)
	for i := 0; i < l.config.MongosCount; i++ {
		// mlaunch assigns ports sequentially
		// mongos instances come after all mongod and config servers
		totalMongods := l.config.Shards * l.config.ReplicaNodes
		mongosPort := l.config.BasePort + totalMongods + l.config.ConfigServers + i
		hosts[i] = fmt.Sprintf("localhost:%d", mongosPort)
	}
	return hosts
}

// GetConnectionString returns a MongoDB connection string for the cluster
func (l *Launcher) GetConnectionString() string {
	hosts := l.GetMongosHosts()
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
