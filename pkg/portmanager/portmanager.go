package portmanager

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// PortRange represents an allocated range of ports
type PortRange struct {
	Base  int
	Count int
	Ports []int
}

// Allocator manages port allocation with conflict detection
type Allocator struct {
	basePort     int
	maxPort      int
	maxRetries   int
	retryBackoff time.Duration
	mu           sync.Mutex
	allocated    map[int]bool
}

// NewAllocator creates a new port allocator
// basePort: starting port for allocation (default: 30000)
// maxPort: maximum port number (default: 40000)
func NewAllocator(basePort, maxPort int) *Allocator {
	if basePort == 0 {
		basePort = 30000
	}
	if maxPort == 0 {
		maxPort = 40000
	}

	return &Allocator{
		basePort:     basePort,
		maxPort:      maxPort,
		maxRetries:   5,
		retryBackoff: 500 * time.Millisecond,
		allocated:    make(map[int]bool),
	}
}

// AllocateRange attempts to allocate a contiguous range of ports
// Returns an error if no available ports are found after retries
func (a *Allocator) AllocateRange(count int) (*PortRange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for attempt := 0; attempt < a.maxRetries; attempt++ {
		base, ports := a.findAvailableRange(count)
		if base == 0 {
			return nil, fmt.Errorf("no contiguous port range of size %d available", count)
		}

		// Verify ports are actually available on the system
		if a.verifyPortsAvailable(ports) {
			// Mark ports as allocated
			for _, port := range ports {
				a.allocated[port] = true
			}

			return &PortRange{
				Base:  base,
				Count: count,
				Ports: ports,
			}, nil
		}

		// Backoff before retry
		a.mu.Unlock()
		time.Sleep(a.retryBackoff * time.Duration(attempt+1))
		a.mu.Lock()
	}

	return nil, fmt.Errorf("failed to allocate %d ports after %d attempts", count, a.maxRetries)
}

// findAvailableRange finds a contiguous range of unallocated ports
func (a *Allocator) findAvailableRange(count int) (int, []int) {
	for base := a.basePort; base+count <= a.maxPort; base++ {
		available := true
		ports := make([]int, count)

		for i := 0; i < count; i++ {
			port := base + i
			if a.allocated[port] {
				available = false
				break
			}
			ports[i] = port
		}

		if available {
			return base, ports
		}
	}

	return 0, nil
}

// verifyPortsAvailable checks if ports are actually available on the system
func (a *Allocator) verifyPortsAvailable(ports []int) bool {
	for _, port := range ports {
		if !isPortAvailable(port) {
			return false
		}
	}
	return true
}

// Release frees the allocated ports
func (a *Allocator) Release(pr *PortRange) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, port := range pr.Ports {
		delete(a.allocated, port)
	}
}

// isPortAvailable checks if a port is available by attempting to connect
func isPortAvailable(port int) bool {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		// Connection failed, port is available
		return true
	}
	// Connection succeeded, port is in use
	conn.Close()
	return false
}

// AllocateSpecificPorts allocates a specific set of ports
func (a *Allocator) AllocateSpecificPorts(ports []int) (*PortRange, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(ports) == 0 {
		return nil, fmt.Errorf("no ports specified")
	}

	// Verify all ports are available on the system
	for _, port := range ports {
		if !isPortAvailable(port) {
			return nil, fmt.Errorf("port %d is not available", port)
		}
		if a.allocated[port] {
			return nil, fmt.Errorf("port %d is already allocated", port)
		}
	}

	// Mark ports as allocated
	for _, port := range ports {
		a.allocated[port] = true
	}

	// Find base port (minimum port in the range)
	base := ports[0]
	for _, port := range ports {
		if port < base {
			base = port
		}
	}

	return &PortRange{
		Base:  base,
		Count: len(ports),
		Ports: ports,
	}, nil
}

// FindSequentialAvailablePorts finds a sequential range of available ports by querying the system
func FindSequentialAvailablePorts(count int, startPort, endPort int) ([]int, error) {
	if startPort == 0 {
		startPort = 30000
	}
	if endPort == 0 {
		endPort = 40000
	}

	if startPort+count > endPort {
		return nil, fmt.Errorf("not enough ports in range %d-%d for %d sequential ports", startPort, endPort, count)
	}

	// Try to find sequential available ports
	for base := startPort; base+count <= endPort; base++ {
		ports := make([]int, count)
		allAvailable := true

		// Check if all ports in this range are available
		for i := 0; i < count; i++ {
			port := base + i
			ports[i] = port
			if !isPortAvailable(port) {
				allAvailable = false
				break
			}
		}

		if allAvailable {
			return ports, nil
		}
	}

	return nil, fmt.Errorf("no sequential range of %d available ports found in range %d-%d", count, startPort, endPort)
}

// CalculatePortsNeeded calculates total ports required for a cluster
func CalculatePortsNeeded(shards, replicaNodes, configServers, mongosCount int) int {
	// Each shard has replicaNodes mongod instances
	// Plus config servers and mongos instances
	return (shards * replicaNodes) + configServers + mongosCount
}
