# Mongo scaffold

A test bed for local testing of MongoDB sharded clusters.

## Components

### 1. Go Test Harness (Recommended)

A Go library for programmatically spinning up isolated MongoDB clusters in tests with automatic port allocation and cleanup.

**Features:**
- Automatic dependency installation (uv, mtools)
- Non-conflicting port allocation for parallel tests
- Temporary directories with automatic cleanup
- Health checks with retry logic
- Support for multiple MongoDB versions

**Quick Start:**
```go
import "github.com/zph/mongo-scaffold/pkg/cluster"

c, _ := cluster.NewCluster(cluster.Config{
    MongoVersion:  "3.6",
    Shards:        2,
    ReplicaNodes:  3,
})
defer c.Teardown()
c.Start()

// Use c.ConnectionString() to connect
```

See [HARNESS.md](./HARNESS.md) for complete documentation.

### 2. Manual Setup (Legacy)

Traditional setup using mlaunch + keyhole + bash.

This project uses [uv](https://github.com/astral-sh/uv) for Python dependency management.

```bash
# Run setup to initialize the environment and cluster
make setup

# Load fake data in keyhole db
make seed

# Connect to mongo sharded cluster
make connect

# Cleanup and stop running cluster
make clean
```

## Requirements

- **Go** 1.19+ (for test harness)
- **m** - MongoDB version manager ([aheckmann/m](https://github.com/aheckmann/m))
- **uv** - Python package installer (auto-installed by harness)
- **mtools** - MongoDB testing utilities (auto-installed by harness)
