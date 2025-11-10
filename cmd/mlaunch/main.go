package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/zph/mongo-scaffold/pkg/mlaunch"
	"github.com/zph/mongo-scaffold/pkg/mongo_version_manager"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "init":
		initCommand()
	case "start":
		startCommand()
	case "stop":
		stopCommand()
	case "kill":
		killCommand()
	case "killall":
		killAllCommand()
	case "reset":
		resetCommand()
	case "list":
		listCommand()
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
		printUsage()
		os.Exit(1)
	}
}

func initCommand() {
	fs := flag.NewFlagSet("init", flag.ExitOnError)

	// Data directory
	dir := fs.String("dir", "./data", "Data directory for cluster (default: ./data)")

	// Preset option
	preset := fs.String("preset", "", "Use a preset configuration: replicaset, cluster, or cluster-with-auth (overrides other options)")

	// Replica set options
	replicaset := fs.Bool("replicaset", false, "Start as replica set")
	// Use -1 as sentinel value to detect if flag was explicitly set
	nodes := fs.Int("nodes", -1, "Number of replica set nodes")

	// Sharding options
	// Use -1 as sentinel value to detect if flag was explicitly set
	sharded := fs.Int("sharded", -1, "Number of shards (0 = no sharding)")
	configServers := fs.Int("config", -1, "Number of config servers")
	mongos := fs.Int("mongos", -1, "Number of mongos routers")

	// Port options
	port := fs.Int("port", 27017, "Base/starting port number (deprecated: use --port-range)")
	portRange := fs.String("port-range", "", "Port range in format 'start-end' or 'start:end' (e.g., '27017-28000')")
	portMin := fs.Int("port-min", 0, "Minimum port number (used if --port-range is not set)")
	portMax := fs.Int("port-max", 0, "Maximum port number (0 = no limit, used if --port-range is not set)")

	// Authentication
	auth := fs.Bool("auth", false, "Enable authentication")
	// Use empty string as sentinel to detect if flag was explicitly set
	// Empty string means "not set", any non-empty value means "explicitly set"
	username := fs.String("username", "", "Authentication username")
	password := fs.String("password", "", "Authentication password")

	// Binary path
	binarypath := fs.String("binarypath", "", "Path to MongoDB binaries (auto-detected if not specified)")

	// MongoDB version (for auto-detection)
	mongoVersion := fs.String("mongodb-version", "", "MongoDB version (e.g., 3.6, 4.0, 5.0) - used for auto-detection")

	// Post-initialization hook
	postInitScript := fs.String("post-init-script", "", "Path to a mongosh JavaScript file to execute after cluster initialization")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch init [options]\n\n")
		fmt.Fprintf(os.Stderr, "Initialize a new MongoDB cluster.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nPresets:\n")
		fmt.Fprintf(os.Stderr, "  --preset replicaset         Start a replica set with 3 nodes\n")
		fmt.Fprintf(os.Stderr, "  --preset cluster            Start a sharded cluster with 3 shards, 3 nodes per shard, 1 config server, 1 mongos\n")
		fmt.Fprintf(os.Stderr, "  --preset cluster-with-auth  Same as cluster but with --auth, --username admin, --password admin\n")
		fmt.Fprintf(os.Stderr, "  All other options can override preset defaults.\n\n")
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "  # Standalone instance (uses ./data by default)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init\n\n")
		fmt.Fprintf(os.Stderr, "  # Replica set\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --replicaset --nodes 3\n\n")
		fmt.Fprintf(os.Stderr, "  # Use preset: replica set with 3 nodes\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset replicaset\n\n")
		fmt.Fprintf(os.Stderr, "  # Use preset: replica set with 5 nodes (override preset)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset replicaset --nodes 5\n\n")
		fmt.Fprintf(os.Stderr, "  # Use preset: sharded cluster\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset cluster\n\n")
		fmt.Fprintf(os.Stderr, "  # Use preset: sharded cluster with 5 shards (override preset)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset cluster --sharded 5\n\n")
		fmt.Fprintf(os.Stderr, "  # Use preset: sharded cluster with authentication\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset cluster-with-auth\n\n")
		fmt.Fprintf(os.Stderr, "  # Sharded cluster (manual configuration)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --replicaset --nodes 3 --sharded 2 --config 1 --mongos 1\n\n")
		fmt.Fprintf(os.Stderr, "  # With authentication\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --replicaset --nodes 3 --auth --username admin --password secret\n\n")
		fmt.Fprintf(os.Stderr, "  # With port range\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --replicaset --nodes 3 --port-range 27017-28000\n\n")
		fmt.Fprintf(os.Stderr, "  # With post-initialization script\n")
		fmt.Fprintf(os.Stderr, "  mlaunch init --preset cluster --post-init-script setup.js\n\n")
	}

	fs.Parse(os.Args[2:])

	// Determine binary path if not provided
	binPath := *binarypath
	if binPath == "" {
		// Try to auto-detect using mongo version manager
		mgr, err := mongo_version_manager.NewManager()
		if err == nil && *mongoVersion != "" {
			binPath, err = mgr.GetBinPath(*mongoVersion)
			if err != nil {
				log.Printf("Warning: failed to get MongoDB bin path for version %s: %v", *mongoVersion, err)
				log.Printf("Attempting to use version from 'm' tool...")
			}
		}

		// If still not found, try to find mongod in PATH
		if binPath == "" {
			// Check common locations
			homeDir, err := os.UserHomeDir()
			if err == nil {
				// Try to find latest version in ~/.m/versions
				versionsDir := filepath.Join(homeDir, ".m", "versions")
				if entries, err := os.ReadDir(versionsDir); err == nil {
					// Use the first available version
					for _, entry := range entries {
						if entry.IsDir() {
							potentialPath := filepath.Join(versionsDir, entry.Name(), "bin")
							if _, err := os.Stat(potentialPath); err == nil {
								binPath = potentialPath
								log.Printf("Using MongoDB binaries from: %s", binPath)
								break
							}
						}
					}
				}
			}

			if binPath == "" {
				fmt.Fprintf(os.Stderr, "Error: --binarypath is required or MongoDB must be installed via 'm' tool\n")
				fmt.Fprintf(os.Stderr, "Install MongoDB using: m <version>\n")
				fmt.Fprintf(os.Stderr, "Or specify --binarypath\n\n")
				os.Exit(1)
			}
		}
	}

	// Set up base configuration based on preset if specified
	var launchConfig mlaunch.Config
	if *preset != "" {
		switch *preset {
		case "replicaset":
			launchConfig = mlaunch.Config{
				DataDir:       *dir,
				Shards:        0,
				ReplicaNodes:  3,
				ConfigServers: 0,
				MongosCount:   0,
				BasePort:      *port,
				PortRange:     *portRange,
				PortMin:       *portMin,
				PortMax:       *portMax,
				Auth:          false,
				Username:      "",
				Password:      "",
				BinPath:       binPath,
			}
		case "cluster":
			launchConfig = mlaunch.Config{
				DataDir:       *dir,
				Shards:        3,
				ReplicaNodes:  3,
				ConfigServers: 1,
				MongosCount:   1,
				BasePort:      *port,
				PortRange:     *portRange,
				PortMin:       *portMin,
				PortMax:       *portMax,
				Auth:          false,
				Username:      "",
				Password:      "",
				BinPath:       binPath,
			}
		case "cluster-with-auth":
			launchConfig = mlaunch.Config{
				DataDir:       *dir,
				Shards:        3,
				ReplicaNodes:  3,
				ConfigServers: 1,
				MongosCount:   1,
				BasePort:      *port,
				PortRange:     *portRange,
				PortMin:       *portMin,
				PortMax:       *portMax,
				Auth:          true,
				Username:      "admin",
				Password:      "admin",
				BinPath:       binPath,
			}
		default:
			fmt.Fprintf(os.Stderr, "Error: unknown preset type: %s\n\n", *preset)
			fmt.Fprintf(os.Stderr, "Valid preset types: replicaset, cluster, cluster-with-auth\n\n")
			fs.Usage()
			os.Exit(1)
		}

		// Override preset defaults with command-line arguments
		// Use -1 as sentinel value to detect if flag was explicitly set
		// Only override if flag value is not -1 (meaning it was explicitly set)

		if *replicaset {
			// If --replicaset is explicitly set, ensure we're in replica set mode
			if launchConfig.Shards == 0 {
				// Already a replica set, but might need to adjust nodes
				// Only override if explicitly set (not -1)
				if *nodes >= 0 {
					launchConfig.ReplicaNodes = *nodes
				}
			} else {
				// Convert from sharded to replica set
				launchConfig.Shards = 0
				launchConfig.ConfigServers = 0
				launchConfig.MongosCount = 0
				if *nodes >= 0 {
					launchConfig.ReplicaNodes = *nodes
				} else {
					launchConfig.ReplicaNodes = 3 // Default for replica set
				}
			}
		} else if *nodes >= 0 {
			// Override replica nodes if explicitly set (not -1)
			launchConfig.ReplicaNodes = *nodes
		}

		// Only override shards if explicitly set (not -1)
		if *sharded >= 0 {
			launchConfig.Shards = *sharded
		}
		// Only override config servers if explicitly set (not -1)
		if *configServers >= 0 {
			launchConfig.ConfigServers = *configServers
		}
		// Only override mongos if explicitly set (not -1)
		if *mongos >= 0 {
			launchConfig.MongosCount = *mongos
		}

		// Override auth settings if explicitly specified
		// Only override if --auth is set, or if username/password are explicitly set (non-empty)
		if *auth {
			// --auth explicitly set, enable auth
			launchConfig.Auth = true
			if *username != "" {
				launchConfig.Username = *username
			}
			if *password != "" {
				launchConfig.Password = *password
			}
		} else if *username != "" || *password != "" {
			// Username/password explicitly set (non-empty) but --auth not set, enable auth
			launchConfig.Auth = true
			if *username != "" {
				launchConfig.Username = *username
			}
			if *password != "" {
				launchConfig.Password = *password
			}
		}

		// Override port settings if specified
		if *portRange != "" {
			launchConfig.PortRange = *portRange
		}
		if *portMin > 0 {
			launchConfig.PortMin = *portMin
		}
		if *portMax > 0 {
			launchConfig.PortMax = *portMax
		}
		if *port != 27017 { // Only override if not default
			launchConfig.BasePort = *port
		}
	} else {
		// No preset specified, use regular init logic
		// Determine replica nodes based on flags
		replicaNodes := 1
		if *replicaset {
			if *nodes >= 0 {
				replicaNodes = *nodes
			}
			if replicaNodes < 1 {
				replicaNodes = 1
			}
		}

		// Use defaults for flags that weren't explicitly set (-1)
		shards := 0
		if *sharded >= 0 {
			shards = *sharded
		}
		configSrv := 1
		if *configServers >= 0 {
			configSrv = *configServers
		}
		mongosCount := 1
		if *mongos >= 0 {
			mongosCount = *mongos
		}

		// Handle auth settings for non-preset mode
		// If username/password are empty (not set), use defaults
		authUsername := *username
		authPassword := *password
		if authUsername == "" {
			authUsername = "admin" // Default username
		}
		if authPassword == "" {
			authPassword = "admin" // Default password
		}

		// Create launcher config
		launchConfig = mlaunch.Config{
			DataDir:       *dir,
			Shards:        shards,
			ReplicaNodes:  replicaNodes,
			ConfigServers: configSrv,
			MongosCount:   mongosCount,
			BasePort:      *port, // For backward compatibility
			PortRange:     *portRange,
			PortMin:       *portMin,
			PortMax:       *portMax,
			Auth:          *auth,
			Username:      authUsername,
			Password:      authPassword,
			BinPath:       binPath,
		}
	}

	// Create and initialize launcher
	launcher := mlaunch.NewNativeLauncher(launchConfig)

	log.Printf("Initializing MongoDB cluster in: %s", *dir)
	if *preset != "" {
		log.Printf("Using preset: %s", *preset)
	}
	log.Printf("Configuration:")
	log.Printf("  Shards: %d", launchConfig.Shards)
	log.Printf("  Replica Nodes: %d", launchConfig.ReplicaNodes)
	log.Printf("  Config Servers: %d", launchConfig.ConfigServers)
	log.Printf("  Mongos Routers: %d", launchConfig.MongosCount)
	if launchConfig.PortRange != "" {
		log.Printf("  Port Range: %s", launchConfig.PortRange)
	} else if launchConfig.PortMin > 0 {
		if launchConfig.PortMax > 0 {
			log.Printf("  Port Range: %d-%d", launchConfig.PortMin, launchConfig.PortMax)
		} else {
			log.Printf("  Starting Port: %d", launchConfig.PortMin)
		}
	} else {
		log.Printf("  Base Port: %d", launchConfig.BasePort)
	}
	log.Printf("  Authentication: %v", launchConfig.Auth)
	if launchConfig.Auth {
		log.Printf("  Username: %s", launchConfig.Username)
		log.Printf("  Password: %s", launchConfig.Password)
	}
	log.Printf("  Binary Path: %s", launchConfig.BinPath)

	if err := launcher.Init(); err != nil {
		log.Fatalf("Failed to initialize cluster: %v", err)
	}

	log.Println("Cluster initialized successfully!")
	log.Printf("Connection string: %s", launcher.GetConnectionString())

	// Execute post-initialization script if provided
	if *postInitScript != "" {
		if err := executePostInitScript(*postInitScript, launcher, binPath, launchConfig); err != nil {
			log.Fatalf("Failed to execute post-initialization script: %v", err)
		}
	}
}

// executePostInitScript executes a mongosh JavaScript file against the cluster
func executePostInitScript(scriptPath string, launcher mlaunch.LauncherInterface, binPath string, config mlaunch.Config) error {
	// Get absolute path to script
	absScriptPath, err := filepath.Abs(scriptPath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for script: %w", err)
	}

	// Check if script file exists
	if _, err := os.Stat(absScriptPath); os.IsNotExist(err) {
		return fmt.Errorf("script file does not exist: %s", absScriptPath)
	}

	log.Printf("Executing post-initialization script: %s", absScriptPath)

	// Try to find mongosh first (MongoDB 4.4+), fall back to mongo (MongoDB 3.6-4.2)
	var shellPath string
	var shellArgs []string

	// Try mongosh first
	mongoshPath := filepath.Join(binPath, "mongosh")
	if runtime.GOOS == "windows" {
		mongoshPath += ".exe"
	}
	if _, err := os.Stat(mongoshPath); err == nil {
		shellPath = mongoshPath
		// mongosh uses --file flag for script execution
		shellArgs = []string{"--file", absScriptPath}
	} else {
		// Fall back to mongo shell (MongoDB 3.6-4.2)
		mongoPath := filepath.Join(binPath, "mongo")
		if runtime.GOOS == "windows" {
			mongoPath += ".exe"
		}
		if _, err := os.Stat(mongoPath); err == nil {
			shellPath = mongoPath
			// mongo shell uses --eval with load() or direct file execution
			// We'll use --eval with load() to execute the script
			shellArgs = []string{"--eval", fmt.Sprintf("load('%s')", absScriptPath)}
		} else {
			return fmt.Errorf("neither mongosh nor mongo shell found in %s", binPath)
		}
	}

	// Get connection string
	connStr := launcher.GetConnectionString()

	// Add connection string to arguments
	// For mongosh: mongosh <connection-string> --file <script>
	// For mongo: mongo <connection-string> --eval "load('<script>')"
	if strings.HasPrefix(shellPath, filepath.Join(binPath, "mongosh")) {
		// mongosh: connection string comes first, then flags
		shellArgs = append([]string{connStr}, shellArgs...)
	} else {
		// mongo: connection string comes first, then --eval
		shellArgs = append([]string{connStr}, shellArgs...)
	}

	// Execute the script
	cmd := exec.Command(shellPath, shellArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	log.Printf("Running: %s %s", shellPath, strings.Join(shellArgs, " "))

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("script execution failed: %w", err)
	}

	log.Println("Post-initialization script executed successfully")
	return nil
}

func stopCommand() {
	fs := flag.NewFlagSet("stop", flag.ExitOnError)
	dir := fs.String("dir", "./data", "Data directory of cluster to stop (default: ./data)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch stop [--dir <directory>]\n\n")
		fmt.Fprintf(os.Stderr, "Stop a running MongoDB cluster gracefully.\n\n")
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[2:])

	// Verify directory exists
	if _, err := os.Stat(*dir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", *dir)
	}

	// Create launcher with minimal config (just data dir)
	launchConfig := mlaunch.Config{
		DataDir: *dir,
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	log.Printf("Stopping cluster in: %s", *dir)
	if err := launcher.Stop(); err != nil {
		log.Fatalf("Failed to stop cluster: %v", err)
	}

	log.Println("Cluster stopped successfully")
}

func startCommand() {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	dir := fs.String("dir", "./data", "Data directory of cluster to start (default: ./data)")
	binarypath := fs.String("binarypath", "", "Path to MongoDB binaries (auto-detected if not specified)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch start [--dir <directory>] [--binarypath <path>]\n\n")
		fmt.Fprintf(os.Stderr, "Start a MongoDB cluster from saved metadata (processes.json).\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Start cluster in default directory (./data)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch start\n\n")
		fmt.Fprintf(os.Stderr, "  # Start cluster in custom directory\n")
		fmt.Fprintf(os.Stderr, "  mlaunch start --dir /path/to/data\n\n")
	}

	fs.Parse(os.Args[2:])

	// Verify directory exists
	if _, err := os.Stat(*dir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", *dir)
	}

	// Verify processes.json exists
	processesFile := filepath.Join(*dir, "processes.json")
	if _, err := os.Stat(processesFile); os.IsNotExist(err) {
		log.Fatalf("processes.json not found in %s. Run 'mlaunch init' first.", *dir)
	}

	// Create launcher with minimal config (just data dir)
	// The binary path will be loaded from processes.json if available
	launchConfig := mlaunch.Config{
		DataDir: *dir,
		BinPath: *binarypath, // Use provided binary path if specified
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	// Load metadata to get binary path if not specified
	if *binarypath == "" {
		if err := launcher.LoadMetadata(); err == nil {
			// Metadata loaded successfully, binary path should be set
			log.Printf("Using MongoDB binaries from saved metadata")
		} else {
			// Try to find mongod in PATH as fallback
			homeDir, err := os.UserHomeDir()
			if err == nil {
				// Try to find latest version in ~/.m/versions
				versionsDir := filepath.Join(homeDir, ".m", "versions")
				if entries, err := os.ReadDir(versionsDir); err == nil {
					// Use the first available version
					for _, entry := range entries {
						if entry.IsDir() {
							potentialPath := filepath.Join(versionsDir, entry.Name(), "bin")
							if _, err := os.Stat(potentialPath); err == nil {
								launchConfig.BinPath = potentialPath
								log.Printf("Using MongoDB binaries from: %s", potentialPath)
								// Update launcher config
								launcher = mlaunch.NewNativeLauncher(launchConfig)
								break
							}
						}
					}
				}
			}

			if launchConfig.BinPath == "" {
				fmt.Fprintf(os.Stderr, "Error: --binarypath is required or MongoDB must be installed via 'm' tool\n")
				fmt.Fprintf(os.Stderr, "Install MongoDB using: m <version>\n")
				fmt.Fprintf(os.Stderr, "Or specify --binarypath\n\n")
				os.Exit(1)
			}
		}
	}

	log.Printf("Starting cluster from: %s", *dir)
	if err := launcher.Start(); err != nil {
		log.Fatalf("Failed to start cluster: %v", err)
	}

	log.Println("Cluster started successfully")
	log.Printf("Connection string: %s", launcher.GetConnectionString())
}

func killCommand() {
	fs := flag.NewFlagSet("kill", flag.ExitOnError)
	dir := fs.String("dir", "./data", "Data directory of cluster to kill (default: ./data)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch kill [--dir <directory>]\n\n")
		fmt.Fprintf(os.Stderr, "Forcefully kill a MongoDB cluster.\n\n")
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[2:])

	// Verify directory exists
	if _, err := os.Stat(*dir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", *dir)
	}

	// Create launcher with minimal config (just data dir)
	launchConfig := mlaunch.Config{
		DataDir: *dir,
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	log.Printf("Killing cluster in: %s", *dir)
	if err := launcher.Kill(); err != nil {
		log.Fatalf("Failed to kill cluster: %v", err)
	}

	log.Println("Cluster killed successfully")
}

func resetCommand() {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	dir := fs.String("dir", "./data", "Data directory of cluster to reset (default: ./data)")
	yes := fs.Bool("yes", false, "Skip confirmation prompt and proceed with deletion")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch reset [--dir <directory>] [--yes]\n\n")
		fmt.Fprintf(os.Stderr, "Stop a MongoDB cluster and remove its data directory.\n")
		fmt.Fprintf(os.Stderr, "This is equivalent to 'mlaunch stop' followed by 'rm -rf <directory>'.\n\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  # Reset cluster in default directory (./data)\n")
		fmt.Fprintf(os.Stderr, "  mlaunch reset\n\n")
		fmt.Fprintf(os.Stderr, "  # Reset cluster in custom directory\n")
		fmt.Fprintf(os.Stderr, "  mlaunch reset --dir /path/to/data\n\n")
		fmt.Fprintf(os.Stderr, "  # Reset without confirmation prompt\n")
		fmt.Fprintf(os.Stderr, "  mlaunch reset --yes\n\n")
	}

	fs.Parse(os.Args[2:])

	// Get absolute path for display
	absDir, err := filepath.Abs(*dir)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	// Confirm deletion unless --yes is set
	if !*yes {
		fmt.Printf("WARNING: This will stop all MongoDB processes and DELETE the data directory:\n")
		fmt.Printf("  %s\n\n", absDir)
		fmt.Printf("This action cannot be undone. Are you sure you want to continue? [y/N]: ")

		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			log.Fatalf("Failed to read input: %v", err)
		}

		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Println("Reset cancelled.")
			os.Exit(0)
		}
	}

	// Create launcher with minimal config (just data dir)
	launchConfig := mlaunch.Config{
		DataDir: *dir,
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	log.Printf("Resetting cluster in: %s", *dir)
	if err := launcher.Reset(); err != nil {
		log.Fatalf("Failed to reset cluster: %v", err)
	}

	log.Println("Cluster reset successfully")
}

func killAllCommand() {
	fs := flag.NewFlagSet("killall", flag.ExitOnError)

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch killall\n\n")
		fmt.Fprintf(os.Stderr, "Kill all mongod and mongos processes on the system.\n")
		fmt.Fprintf(os.Stderr, "This is a helper command to clean up any running MongoDB processes.\n\n")
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[2:])

	// Create launcher with minimal config (no data dir needed for killall)
	launchConfig := mlaunch.Config{
		DataDir: "", // Not needed for killall
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	log.Println("Killing all mongod and mongos processes...")
	if err := launcher.KillAll(); err != nil {
		log.Fatalf("Failed to kill all processes: %v", err)
	}

	log.Println("All mongod and mongos processes killed successfully")
}

func listCommand() {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	dir := fs.String("dir", "./data", "Data directory of cluster to list (default: ./data)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: mlaunch list [--dir <directory>]\n\n")
		fmt.Fprintf(os.Stderr, "List running MongoDB cluster processes.\n\n")
		fs.PrintDefaults()
	}

	fs.Parse(os.Args[2:])

	// Verify directory exists
	if _, err := os.Stat(*dir); os.IsNotExist(err) {
		log.Fatalf("Directory does not exist: %s", *dir)
	}

	// Create launcher with minimal config (just data dir)
	launchConfig := mlaunch.Config{
		DataDir: *dir,
	}

	launcher := mlaunch.NewNativeLauncher(launchConfig)

	output, err := launcher.List()
	if err != nil {
		log.Fatalf("Failed to list processes: %v", err)
	}

	fmt.Print(output)
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: mlaunch <command> [options]\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  init     Initialize a new MongoDB cluster\n")
	fmt.Fprintf(os.Stderr, "  start    Start a cluster from saved metadata (processes.json)\n")
	fmt.Fprintf(os.Stderr, "  stop     Stop a running cluster gracefully\n")
	fmt.Fprintf(os.Stderr, "  kill     Forcefully kill a running cluster\n")
	fmt.Fprintf(os.Stderr, "  killall  Kill all mongod and mongos processes on the system\n")
	fmt.Fprintf(os.Stderr, "  reset    Stop cluster and remove data directory\n")
	fmt.Fprintf(os.Stderr, "  list     List running cluster processes\n")
	fmt.Fprintf(os.Stderr, "  help     Show this help message\n\n")
	fmt.Fprintf(os.Stderr, "Use 'mlaunch <command> --help' for command-specific help.\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  mlaunch init --replicaset --nodes 3\n")
	fmt.Fprintf(os.Stderr, "  mlaunch init --preset replicaset\n")
	fmt.Fprintf(os.Stderr, "  mlaunch init --preset cluster-with-auth\n")
	fmt.Fprintf(os.Stderr, "  mlaunch start\n")
	fmt.Fprintf(os.Stderr, "  mlaunch stop\n")
	fmt.Fprintf(os.Stderr, "  mlaunch list\n")
}
