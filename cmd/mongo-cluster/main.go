package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zph/mongo-scaffold/pkg/mongocluster"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "start":
		startCommand()
	case "stop":
		stopCommand()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: %s <command> [options]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "A CLI for spinning up MongoDB test clusters.\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  start    Start a MongoDB cluster\n")
	fmt.Fprintf(os.Stderr, "  stop     Stop a running MongoDB cluster\n")
	fmt.Fprintf(os.Stderr, "  help     Show this help message\n\n")
	fmt.Fprintf(os.Stderr, "Use '%s <command> --help' for command-specific help.\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  %s start\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s start --background\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s stop --file cluster.json\n", os.Args[0])
}

func startCommand() {
	fs := flag.NewFlagSet("start", flag.ExitOnError)

	// Define flags
	mongoVersion := fs.String("version", mongocluster.DefaultMongoVersion, "MongoDB version (e.g., 3.6, 4.0, 5.0)")
	shards := fs.Int("shards", mongocluster.DefaultShards, "Number of shards")
	replicaNodes := fs.Int("nodes", mongocluster.DefaultReplicaNodes, "Replica set members per shard")
	configServers := fs.Int("config", mongocluster.DefaultConfigServers, "Number of config servers")
	mongosCount := fs.Int("mongos", mongocluster.DefaultMongosCount, "Number of mongos routers")
	auth := fs.Bool("auth", false, "Enable authentication")
	username := fs.String("username", "admin", "Authentication username")
	password := fs.String("password", "admin", "Authentication password")
	background := fs.Bool("background", false, "Start cluster in background (default: false)")
	timeout := fs.Duration("timeout", 60*time.Second, "Health check timeout")
	startupTimeout := fs.Duration("startup-timeout", 0, "Maximum time to wait for cluster startup (0 = no timeout)")
	outputFile := fs.String("file", "", "Output file path for cluster information (JSON format)")
	fileOverwrite := fs.Bool("file-overwrite", false, "If true, delete existing --file if it exists (default: false)")
	keepTempDir := fs.Bool("keep-temp-dir", false, "If true, don't delete temp directory on teardown (for debugging)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s start [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Start a MongoDB test cluster.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Start a basic cluster\n")
		fmt.Fprintf(os.Stderr, "  %s start\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Start in background\n")
		fmt.Fprintf(os.Stderr, "  %s start --background\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Start with authentication\n")
		fmt.Fprintf(os.Stderr, "  %s start -auth -username admin -password secret\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Start a larger cluster\n")
		fmt.Fprintf(os.Stderr, "  %s start -shards 3 -nodes 5 -mongos 2\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Use a different MongoDB version\n")
		fmt.Fprintf(os.Stderr, "  %s start -version 5.0\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Output cluster information to file\n")
		fmt.Fprintf(os.Stderr, "  %s start -file cluster-info.json\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Set startup timeout\n")
		fmt.Fprintf(os.Stderr, "  %s start -startup-timeout 5m\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Overwrite existing cluster info file\n")
		fmt.Fprintf(os.Stderr, "  %s start -file cluster.json --file-overwrite\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  # Keep temp directory for debugging\n")
		fmt.Fprintf(os.Stderr, "  %s start --keep-temp-dir\n\n", os.Args[0])
	}

	fs.Parse(os.Args[2:])

	// Create start configuration
	cfg := mongocluster.StartConfig{
		MongoVersion:   *mongoVersion,
		Shards:         *shards,
		ReplicaNodes:   *replicaNodes,
		ConfigServers:  *configServers,
		MongosCount:    *mongosCount,
		Auth:           *auth,
		Username:       *username,
		Password:       *password,
		Timeout:        *timeout,
		StartupTimeout: *startupTimeout,
		OutputFile:     *outputFile,
		FileOverwrite:  *fileOverwrite,
		Background:     *background,
		KeepTempDir:    *keepTempDir,
	}

	// Start the cluster
	result, err := mongocluster.Start(cfg, nil)
	if err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}

	if *background {
		// Background mode: just exit after starting
		log.Println("Cluster started in background mode.")
		return
	}

	// Foreground mode: wait for user input or signals
	if err := mongocluster.WaitForStop(result.Cluster, log.Default()); err != nil {
		log.Fatalf("Error waiting for stop: %v", err)
	}
}

func stopCommand() {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)

	// Define flags
	file := fs.String("file", "", "Path to cluster information file (JSON format)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s stop [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Stop a running MongoDB cluster.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Stop cluster using cluster info file\n")
		fmt.Fprintf(os.Stderr, "  %s stop --file cluster.json\n\n", os.Args[0])
	}

	fs.Parse(os.Args[2:])

	if *file == "" {
		fmt.Fprintf(os.Stderr, "Error: --file flag is required\n\n")
		fs.Usage()
		os.Exit(1)
	}

	// Stop the cluster
	if err := mongocluster.Stop(*file, log.Default()); err != nil {
		log.Fatalf("Failed to stop cluster: %v", err)
	}
}
