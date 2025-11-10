package healthcheck

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

// Checker performs health checks on a MongoDB cluster
type Checker struct {
	mongosHosts   []string
	connString    string
	timeout       time.Duration
	checkInterval time.Duration
	maxRetries    int
}

// NewChecker creates a new health checker
func NewChecker(mongosHosts []string, connString string, timeout time.Duration, maxRetries int) *Checker {
	if timeout == 0 {
		timeout = 60 * time.Second
	}
	if maxRetries == 0 {
		maxRetries = 10
	}

	return &Checker{
		mongosHosts:   mongosHosts,
		connString:    connString,
		timeout:       timeout,
		checkInterval: 2 * time.Second,
		maxRetries:    maxRetries,
	}
}

// WaitForHealthy waits for the cluster to become healthy
func (c *Checker) WaitForHealthy() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	checks := []struct {
		name string
		fn   func() error
	}{
		{"port binding", c.checkPortsListening},
		{"mongodb connection", c.checkMongoConnection},
		{"server status", c.checkServerStatus},
	}

	for i := 0; i < c.maxRetries; i++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("health check timeout after %v", c.timeout)
		default:
		}

		allHealthy := true
		for _, check := range checks {
			if err := check.fn(); err != nil {
				log.Printf("Health check '%s' failed (attempt %d/%d): %v", check.name, i+1, c.maxRetries, err)
				allHealthy = false
				break
			}
		}

		if allHealthy {
			log.Println("All health checks passed")
			return nil
		}

		time.Sleep(c.checkInterval)
	}

	return fmt.Errorf("cluster unhealthy after %d attempts", c.maxRetries)
}

// checkPortsListening verifies that mongos instances are listening on expected ports
func (c *Checker) checkPortsListening() error {
	for _, host := range c.mongosHosts {
		conn, err := net.DialTimeout("tcp", host, 1*time.Second)
		if err != nil {
			return fmt.Errorf("port %s not listening: %w", host, err)
		}
		conn.Close()
	}
	return nil
}

// checkMongoConnection verifies that we can connect to MongoDB using the driver
func (c *Checker) checkMongoConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(c.connString).
		SetServerSelectionTimeout(3 * time.Second).
		SetConnectTimeout(3 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}
	defer client.Disconnect(ctx)

	// Ping the server
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return fmt.Errorf("failed to ping MongoDB: %w", err)
	}

	return nil
}

// checkServerStatus verifies the MongoDB server status
func (c *Checker) checkServerStatus() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientOpts := options.Client().
		ApplyURI(c.connString).
		SetServerSelectionTimeout(3 * time.Second).
		SetConnectTimeout(3 * time.Second)

	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Disconnect(ctx)

	// Run serverStatus command
	var result map[string]interface{}
	err = client.Database("admin").RunCommand(ctx, map[string]interface{}{
		"serverStatus": 1,
	}).Decode(&result)
	if err != nil {
		return fmt.Errorf("failed to get server status: %w", err)
	}

	// Check if connections are being accepted
	if connections, ok := result["connections"].(map[string]interface{}); ok {
		if current, ok := connections["current"].(int32); ok && current >= 0 {
			return nil
		}
	}

	return fmt.Errorf("server status check failed: unexpected response")
}

// CheckPortAvailable checks if a specific port is available
func CheckPortAvailable(port int) error {
	addr := fmt.Sprintf("localhost:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		// Connection failed, port is not listening
		return fmt.Errorf("port %d is not listening", port)
	}
	conn.Close()
	return nil
}

// WaitForPort waits for a specific port to start listening
func WaitForPort(port int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for port %d", port)
		case <-ticker.C:
			if CheckPortAvailable(port) == nil {
				return nil
			}
		}
	}
}
