package cluster

import (
	"fmt"
	"log"
	"time"

	"github.com/zph/mongo-scaffold/pkg/healthcheck"
	"github.com/zph/mongo-scaffold/pkg/mlaunch"
	"github.com/zph/mongo-scaffold/pkg/mwrapper"
	"github.com/zph/mongo-scaffold/pkg/portmanager"
	"github.com/zph/mongo-scaffold/pkg/tempdir"
)

// Config represents the configuration for a MongoDB test cluster
type Config struct {
	// MongoDB version (e.g., "3.6", "4.0", "5.0")
	MongoVersion string

	// Cluster topology
	Shards        int
	ReplicaNodes  int
	ConfigServers int
	MongosCount   int

	// Authentication
	Auth     bool
	Username string
	Password string

	// Retry and health check configuration
	MaxStartupRetries  int
	HealthCheckTimeout time.Duration
	HealthCheckRetries int
	PortRetries        int
	StartupBackoff     time.Duration
}

// Cluster represents a running MongoDB test cluster
type Cluster struct {
	config        Config
	tempDir       *tempdir.Manager
	portAllocator *portmanager.Allocator
	portRange     *portmanager.PortRange
	launcher      *mlaunch.Launcher
	healthChecker *healthcheck.Checker
	mongosHosts   []string
}

// NewCluster creates a new MongoDB test cluster
func NewCluster(config Config) (*Cluster, error) {
	// Set defaults
	if config.MaxStartupRetries == 0 {
		config.MaxStartupRetries = 3
	}
	if config.HealthCheckTimeout == 0 {
		config.HealthCheckTimeout = 60 * time.Second
	}
	if config.HealthCheckRetries == 0 {
		config.HealthCheckRetries = 10
	}
	if config.StartupBackoff == 0 {
		config.StartupBackoff = 2 * time.Second
	}
	if config.MongosCount == 0 {
		config.MongosCount = 1
	}
	if config.ConfigServers == 0 {
		config.ConfigServers = 1
	}

	// Ensure dependencies are installed
	installer := mlaunch.NewInstaller()
	if err := installer.EnsureDependencies(); err != nil {
		return nil, fmt.Errorf("failed to ensure dependencies: %w", err)
	}

	// Create temp directory
	tempDir, err := tempdir.NewManager()
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	if err := tempDir.CreateClusterDir(); err != nil {
		tempDir.Cleanup()
		return nil, fmt.Errorf("failed to setup cluster directory: %w", err)
	}

	cluster := &Cluster{
		config:        config,
		tempDir:       tempDir,
		portAllocator: portmanager.NewAllocator(30000, 40000),
	}

	return cluster, nil
}

// Start starts the cluster with retry logic
func (c *Cluster) Start() error {
	return c.StartWithRetry()
}

// StartWithRetry starts the cluster with automatic retry on failure
func (c *Cluster) StartWithRetry() error {
	var lastErr error

	for attempt := 0; attempt < c.config.MaxStartupRetries; attempt++ {
		log.Printf("Starting cluster (attempt %d/%d)...", attempt+1, c.config.MaxStartupRetries)

		// Allocate ports with conflict detection
		portsNeeded := portmanager.CalculatePortsNeeded(
			c.config.Shards,
			c.config.ReplicaNodes,
			c.config.ConfigServers,
			c.config.MongosCount,
		)

		portRange, err := c.portAllocator.AllocateRange(portsNeeded)
		if err != nil {
			lastErr = fmt.Errorf("port allocation failed: %w", err)
			log.Printf("Attempt %d failed: %v", attempt+1, lastErr)
			time.Sleep(c.retryBackoff(attempt))
			continue
		}

		c.portRange = portRange

		// Start cluster
		if err := c.startCluster(); err != nil {
			c.cleanup()
			lastErr = fmt.Errorf("cluster start failed: %w", err)
			log.Printf("Attempt %d failed: %v", attempt+1, lastErr)
			time.Sleep(c.retryBackoff(attempt))
			continue
		}

		// Health check with retries
		if err := c.waitForHealthy(); err != nil {
			c.Teardown()
			lastErr = fmt.Errorf("health check failed: %w", err)
			log.Printf("Attempt %d failed: %v", attempt+1, lastErr)
			time.Sleep(c.retryBackoff(attempt))
			continue
		}

		// Save metadata
		if err := c.saveMetadata(); err != nil {
			log.Printf("Warning: failed to save metadata: %v", err)
		}

		log.Println("Cluster started successfully!")
		return nil
	}

	return fmt.Errorf("failed after %d attempts: %w", c.config.MaxStartupRetries, lastErr)
}

// startCluster performs the actual cluster startup
func (c *Cluster) startCluster() error {
	// Ensure MongoDB version is installed
	mongoMgr, err := mwrapper.NewManager()
	if err != nil {
		return fmt.Errorf("failed to initialize m manager: %w", err)
	}

	binPath, err := mongoMgr.GetBinPath(c.config.MongoVersion)
	if err != nil {
		// Try to install the version
		log.Printf("MongoDB %s not found, installing...", c.config.MongoVersion)
		if _, err := mongoMgr.EnsureVersion(c.config.MongoVersion); err != nil {
			return fmt.Errorf("failed to ensure MongoDB version: %w", err)
		}
		binPath, err = mongoMgr.GetBinPath(c.config.MongoVersion)
		if err != nil {
			return fmt.Errorf("failed to get MongoDB bin path: %w", err)
		}
	}

	// Get mlaunch path
	installer := mlaunch.NewInstaller()
	if err := installer.EnsureDependencies(); err != nil {
		return fmt.Errorf("failed to ensure mlaunch: %w", err)
	}

	// Configure mlaunch
	launchConfig := mlaunch.Config{
		DataDir:       c.tempDir.DataDir(),
		Shards:        c.config.Shards,
		ReplicaNodes:  c.config.ReplicaNodes,
		ConfigServers: c.config.ConfigServers,
		MongosCount:   c.config.MongosCount,
		BasePort:      c.portRange.Base,
		Auth:          c.config.Auth,
		Username:      c.config.Username,
		Password:      c.config.Password,
		BinPath:       binPath,
	}

	c.launcher = mlaunch.NewLauncher(installer.GetMlaunchPath(), launchConfig)

	// Initialize cluster
	if err := c.launcher.Init(); err != nil {
		return fmt.Errorf("failed to initialize cluster: %w", err)
	}

	// Get mongos hosts
	c.mongosHosts = c.launcher.GetMongosHosts()

	return nil
}

// waitForHealthy performs health checks on the cluster
func (c *Cluster) waitForHealthy() error {
	connString := c.launcher.GetConnectionString()

	c.healthChecker = healthcheck.NewChecker(
		c.mongosHosts,
		connString,
		c.config.HealthCheckTimeout,
		c.config.HealthCheckRetries,
	)

	return c.healthChecker.WaitForHealthy()
}

// saveMetadata saves cluster metadata to disk
func (c *Cluster) saveMetadata() error {
	meta := tempdir.Metadata{
		Ports:       c.portRange.Ports,
		Version:     c.config.MongoVersion,
		CreatedAt:   time.Now().Format(time.RFC3339),
		MongosHosts: c.mongosHosts,
	}

	return c.tempDir.WriteMetadata(meta)
}

// cleanup performs partial cleanup after a failed start
func (c *Cluster) cleanup() {
	if c.launcher != nil {
		c.launcher.Kill()
	}
	if c.portRange != nil {
		c.portAllocator.Release(c.portRange)
	}
}

// Teardown stops the cluster and cleans up all resources
func (c *Cluster) Teardown() error {
	log.Println("Tearing down cluster...")

	// Stop the cluster
	if c.launcher != nil {
		if err := c.launcher.Stop(); err != nil {
			log.Printf("Warning: failed to stop cluster gracefully: %v", err)
			// Try to kill it
			if err := c.launcher.Kill(); err != nil {
				log.Printf("Warning: failed to kill cluster: %v", err)
			}
		}
	}

	// Release ports
	if c.portRange != nil {
		c.portAllocator.Release(c.portRange)
	}

	// Clean up temp directory
	if err := c.tempDir.Cleanup(); err != nil {
		return fmt.Errorf("failed to cleanup temp directory: %w", err)
	}

	log.Println("Cluster teardown complete")
	return nil
}

// ConnectionString returns the MongoDB connection string for the cluster
func (c *Cluster) ConnectionString() string {
	if c.launcher == nil {
		return ""
	}
	return c.launcher.GetConnectionString()
}

// MongosHosts returns the list of mongos host:port addresses
func (c *Cluster) MongosHosts() []string {
	return c.mongosHosts
}

// DataDir returns the data directory path
func (c *Cluster) DataDir() string {
	return c.tempDir.DataDir()
}

// LogDir returns the logs directory path
func (c *Cluster) LogDir() string {
	return c.tempDir.LogDir()
}

// retryBackoff calculates exponential backoff duration
func (c *Cluster) retryBackoff(attempt int) time.Duration {
	// Exponential backoff: 2s, 4s, 8s, capped at 30s
	duration := c.config.StartupBackoff * time.Duration(1<<uint(attempt))
	if duration > 30*time.Second {
		return 30 * time.Second
	}
	return duration
}
