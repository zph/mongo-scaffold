package cluster

import (
	"log"
	"testing"
)

// TestBasicCluster demonstrates basic cluster usage
func TestBasicCluster(t *testing.T) {
	// Create a minimal sharded cluster
	config := Config{
		MongoVersion:  "3.6",
		Shards:        2,
		ReplicaNodes:  2,
		ConfigServers: 1,
		MongosCount:   1,
		Auth:          false,
	}

	c, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	// Ensure cleanup happens
	defer func() {
		if err := c.Teardown(); err != nil {
			t.Errorf("Failed to teardown cluster: %v", err)
		}
	}()

	// Start the cluster
	if err := c.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	// Get connection string
	connStr := c.ConnectionString()
	log.Printf("Cluster started successfully!")
	log.Printf("Connection string: %s", connStr)
	log.Printf("Data directory: %s", c.DataDir())
	log.Printf("Log directory: %s", c.LogDir())

	// At this point, you can use the cluster for your tests
	// For example, connect with a MongoDB driver and run queries

	// Example placeholder for actual test logic:
	// client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(connStr))
	// if err != nil {
	//     t.Fatalf("Failed to connect to cluster: %v", err)
	// }
	// defer client.Disconnect(context.Background())
	//
	// // Run your test queries here
	// db := client.Database("test")
	// collection := db.Collection("test_collection")
	// // ... perform operations ...
}

// TestClusterWithAuth demonstrates cluster with authentication
func TestClusterWithAuth(t *testing.T) {
	config := Config{
		MongoVersion:  "3.6",
		Shards:        1,
		ReplicaNodes:  2,
		ConfigServers: 1,
		MongosCount:   1,
		Auth:          true,
		Username:      "testuser",
		Password:      "testpass",
	}

	c, err := NewCluster(config)
	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}
	defer c.Teardown()

	if err := c.Start(); err != nil {
		t.Fatalf("Failed to start cluster: %v", err)
	}

	connStr := c.ConnectionString()
	log.Printf("Authenticated cluster connection: %s", connStr)

	// Your authenticated test logic here
}

// TestParallelClusters demonstrates running multiple clusters in parallel
func TestParallelClusters(t *testing.T) {
	t.Run("Cluster1", func(t *testing.T) {
		t.Parallel()

		config := Config{
			MongoVersion:  "3.6",
			Shards:        1,
			ReplicaNodes:  1,
			ConfigServers: 1,
			MongosCount:   1,
		}

		c, err := NewCluster(config)
		if err != nil {
			t.Fatalf("Failed to create cluster 1: %v", err)
		}
		defer c.Teardown()

		if err := c.Start(); err != nil {
			t.Fatalf("Failed to start cluster 1: %v", err)
		}

		log.Printf("Cluster 1: %s", c.ConnectionString())
	})

	t.Run("Cluster2", func(t *testing.T) {
		t.Parallel()

		config := Config{
			MongoVersion:  "3.6",
			Shards:        1,
			ReplicaNodes:  1,
			ConfigServers: 1,
			MongosCount:   1,
		}

		c, err := NewCluster(config)
		if err != nil {
			t.Fatalf("Failed to create cluster 2: %v", err)
		}
		defer c.Teardown()

		if err := c.Start(); err != nil {
			t.Fatalf("Failed to start cluster 2: %v", err)
		}

		log.Printf("Cluster 2: %s", c.ConnectionString())
	})
}
