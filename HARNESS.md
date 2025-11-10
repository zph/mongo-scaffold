# MongoDB Test Harness

A Go test harness for spinning up isolated MongoDB sharded clusters with automatic port allocation, dependency management, and cleanup.

## Features

- **Automatic Dependency Installation**: Installs `uv` and `mtools` if not present
- **MongoDB Version Management**: Uses `m` to manage multiple MongoDB versions
- **Port Conflict Resolution**: Automatically allocates non-conflicting port ranges
- **Parallel Test Support**: Run multiple clusters simultaneously without conflicts
- **Temporary Isolation**: Each cluster gets its own temp directory with automatic cleanup
- **Health Checks**: Waits for cluster readiness with configurable retries
- **Retry Logic**: Exponential backoff for transient failures
- **Graceful Cleanup**: Signal handlers ensure temp directories are cleaned up

## Quick Start

```go
package mytest

import (
    "testing"
    "github.com/zph/mongo-scaffold/pkg/cluster"
)

func TestMyFeature(t *testing.T) {
    // Create a minimal sharded cluster
    config := cluster.Config{
        MongoVersion:  "3.6",
        Shards:        2,
        ReplicaNodes:  3,
        ConfigServers: 1,
        MongosCount:   1,
    }

    c, err := cluster.NewCluster(config)
    if err != nil {
        t.Fatalf("Failed to create cluster: %v", err)
    }
    defer c.Teardown()

    // Start the cluster (with automatic retries)
    if err := c.Start(); err != nil {
        t.Fatalf("Failed to start cluster: %v", err)
    }

    // Use the cluster in your tests
    connStr := c.ConnectionString()
    // ... connect and run tests ...
}
```

## Configuration Options

```go
type Config struct {
    // MongoDB version (e.g., "3.6", "4.0", "5.0", "6.0")
    MongoVersion string

    // Cluster topology
    Shards        int  // Number of shards
    ReplicaNodes  int  // Replica set members per shard
    ConfigServers int  // Config server count (default: 1)
    MongosCount   int  // Mongos router count (default: 1)

    // Authentication (optional)
    Auth     bool
    Username string  // Default: "admin"
    Password string  // Default: "admin"

    // Advanced: Retry and health check configuration
    MaxStartupRetries  int           // Default: 3
    HealthCheckTimeout time.Duration // Default: 60s
    HealthCheckRetries int           // Default: 10
    PortRetries        int           // Default: 5
    StartupBackoff     time.Duration // Default: 2s
}
```

## Examples

### Basic Sharded Cluster

```go
func TestBasic(t *testing.T) {
    c, err := cluster.NewCluster(cluster.Config{
        MongoVersion:  "4.0",
        Shards:        2,
        ReplicaNodes:  2,
    })
    if err != nil {
        t.Fatal(err)
    }
    defer c.Teardown()

    if err := c.Start(); err != nil {
        t.Fatal(err)
    }

    // Your test code here
}
```

### Cluster with Authentication

```go
func TestWithAuth(t *testing.T) {
    c, err := cluster.NewCluster(cluster.Config{
        MongoVersion:  "5.0",
        Shards:        1,
        ReplicaNodes:  3,
        Auth:          true,
        Username:      "admin",
        Password:      "secret",
    })
    if err != nil {
        t.Fatal(err)
    }
    defer c.Teardown()

    if err := c.Start(); err != nil {
        t.Fatal(err)
    }

    // Connection string includes auth: mongodb://admin:secret@localhost:30000/admin
    connStr := c.ConnectionString()
}
```

### Parallel Tests

The harness automatically handles port allocation, so you can run tests in parallel:

```go
func TestParallel(t *testing.T) {
    t.Run("Test1", func(t *testing.T) {
        t.Parallel()

        c, _ := cluster.NewCluster(cluster.Config{
            MongoVersion: "3.6",
            Shards:       1,
            ReplicaNodes: 1,
        })
        defer c.Teardown()
        c.Start()

        // Test 1 logic
    })

    t.Run("Test2", func(t *testing.T) {
        t.Parallel()

        c, _ := cluster.NewCluster(cluster.Config{
            MongoVersion: "4.0",
            Shards:       2,
            ReplicaNodes: 2,
        })
        defer c.Teardown()
        c.Start()

        // Test 2 logic
    })
}
```

## How It Works

### 1. Dependency Management

On first use, the harness:
- Checks if `uv` is installed (installs if needed)
- Uses `uv tool install` to install `mtools[mlaunch]` with Python 3.10
- Checks if `m` (MongoDB version manager) is available
- Downloads and installs requested MongoDB version if needed

### 2. Port Allocation

- Allocates contiguous port ranges starting at 30000
- Verifies ports are actually available (not just unallocated)
- Retries with backoff if conflicts are detected
- Releases ports on cluster teardown

Port calculation:
```
Total ports = (Shards × ReplicaNodes) + ConfigServers + MongosCount
```

### 3. Temporary Directories

Each cluster gets:
```
/tmp/mongo-test-harness-{uuid}/
├── data/              # mlaunch data directory
│   ├── mongod_0/
│   ├── mongod_1/
│   ├── config_0/
│   └── mongos_0/
├── logs/              # All log files
└── metadata.json      # Cluster info
```

Cleanup is automatic via:
- `defer cluster.Teardown()` in tests
- Signal handlers (SIGINT, SIGTERM)
- OS temp directory cleanup

### 4. Health Checks

The harness waits for:
1. Ports to start listening
2. Configurable timeout (default: 60s)
3. Retries with backoff

### 5. Retry Logic

Failures are retried with exponential backoff:
- Port conflicts → Allocate new range
- Startup failures → Kill processes, cleanup, retry
- Health check failures → Full teardown, retry

Backoff: 2s, 4s, 8s (capped at 30s)

## API Reference

### Cluster Methods

```go
// Start the cluster with automatic retries
func (c *Cluster) Start() error

// Get MongoDB connection string
func (c *Cluster) ConnectionString() string

// Get mongos host addresses
func (c *Cluster) MongosHosts() []string

// Get data directory path
func (c *Cluster) DataDir() string

// Get logs directory path
func (c *Cluster) LogDir() string

// Stop cluster and cleanup all resources
func (c *Cluster) Teardown() error
```

## Troubleshooting

### Cluster fails to start

Check logs in the temp directory:
```go
c, _ := cluster.NewCluster(config)
c.Start()
fmt.Printf("Logs: %s\n", c.LogDir())
```

### Port conflicts persist

Increase retry settings:
```go
config := cluster.Config{
    MongoVersion:      "3.6",
    Shards:            2,
    ReplicaNodes:      2,
    MaxStartupRetries: 5,
    PortRetries:       10,
}
```

### MongoDB version not found

Ensure `m` is installed and the version exists:
```bash
m ls           # List installed versions
m 3.6          # Install MongoDB 3.6
```

## Dependencies

- **m**: MongoDB version manager ([github.com/aheckmann/m](https://github.com/aheckmann/m))
- **uv**: Python package installer ([astral.sh/uv](https://astral.sh/uv))
- **mtools**: MongoDB test utilities ([github.com/rueckstiess/mtools](https://github.com/rueckstiess/mtools))

The harness automatically installs `uv` and `mtools` if not present. You must install `m` manually.

## Contributing

See [README.md](./README.md) for development setup.

## License

MIT
