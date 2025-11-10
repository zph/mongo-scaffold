package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/zph/mongo-scaffold/pkg/mongo_version_manager"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "ls", "list":
		listCommand()
	case "use":
		if len(os.Args) < 3 {
			fmt.Fprintf(os.Stderr, "Error: version required for 'use' command\n\n")
			fmt.Fprintf(os.Stderr, "Usage: m use <version>\n\n")
			os.Exit(1)
		}
		useCommand(os.Args[2])
	case "bin", "which":
		if len(os.Args) < 3 {
			fmt.Fprintf(os.Stderr, "Error: version required for 'bin' command\n\n")
			fmt.Fprintf(os.Stderr, "Usage: m bin <version>\n\n")
			os.Exit(1)
		}
		binCommand(os.Args[2])
	case "implode":
		implodeCommand()
	case "help", "--help", "-h":
		printUsage()
	default:
		// If it looks like a version (e.g., 3.6, 4.0, 5.0), install it
		if isVersion(command) {
			installCommand(command)
		} else {
			fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", command)
			printUsage()
			os.Exit(1)
		}
	}
}

func listCommand() {
	mgr, err := mongo_version_manager.NewManager()
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	versions, err := mgr.ListInstalledVersions()
	if err != nil {
		log.Fatalf("Failed to list versions: %v", err)
	}

	if len(versions) == 0 {
		fmt.Println("No MongoDB versions installed")
		return
	}

	for _, version := range versions {
		fmt.Println(version)
	}
}

func installCommand(version string) {
	mgr, err := mongo_version_manager.NewManager()
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Check if already installed
	installed, err := mgr.IsVersionInstalled(version)
	if err != nil {
		log.Fatalf("Failed to check if version is installed: %v", err)
	}

	if installed {
		fmt.Printf("MongoDB %s is already installed\n", version)
		return
	}

	// Get bin path (will install if needed)
	binPath, err := mgr.GetBinPath(version)
	if err != nil {
		log.Fatalf("Failed to install MongoDB %s: %v", version, err)
	}

	fmt.Printf("MongoDB %s installed successfully\n", version)
	fmt.Printf("Bin path: %s\n", binPath)
}

func useCommand(version string) {
	mgr, err := mongo_version_manager.NewManager()
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	if err := mgr.UseVersion(version); err != nil {
		log.Fatalf("Failed to use MongoDB %s: %v", version, err)
	}
}

func binCommand(version string) {
	mgr, err := mongo_version_manager.NewManager()
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	binPath, err := mgr.GetBinPath(version)
	if err != nil {
		log.Fatalf("Failed to get bin path for MongoDB %s: %v", version, err)
	}

	// Ensure the path is absolute
	absPath, err := filepath.Abs(binPath)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	fmt.Println(absPath)
}

func implodeCommand() {
	mgr, err := mongo_version_manager.NewManager()
	if err != nil {
		log.Fatalf("Failed to create manager: %v", err)
	}
	defer mgr.Close()

	if err := mgr.Uninstall(); err != nil {
		log.Fatalf("Failed to implode: %v", err)
	}

	fmt.Println("Imploded successfully")
}

func isVersion(s string) bool {
	// Check if string looks like a version (e.g., "3.6", "4.0", "5.0")
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return false
	}

	// Check if all parts are numeric
	for _, part := range parts {
		if len(part) == 0 {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}

	return true
}

func printUsage() {
	fmt.Fprintf(os.Stderr, "Usage: m <command> [options]\n\n")
	fmt.Fprintf(os.Stderr, "MongoDB Version Manager\n\n")
	fmt.Fprintf(os.Stderr, "Commands:\n")
	fmt.Fprintf(os.Stderr, "  <version>    Install a MongoDB version (e.g., 3.6, 4.0, 5.0)\n")
	fmt.Fprintf(os.Stderr, "  ls           List installed MongoDB versions\n")
	fmt.Fprintf(os.Stderr, "  use <version> Verify a MongoDB version is available\n")
	fmt.Fprintf(os.Stderr, "  bin <version> Output the bin directory path for a version\n")
	fmt.Fprintf(os.Stderr, "  implode      Remove all installed MongoDB versions and clean up\n")
	fmt.Fprintf(os.Stderr, "  help         Show this help message\n\n")
	fmt.Fprintf(os.Stderr, "Examples:\n")
	fmt.Fprintf(os.Stderr, "  m 3.6          # Install MongoDB 3.6\n")
	fmt.Fprintf(os.Stderr, "  m ls            # List installed versions\n")
	fmt.Fprintf(os.Stderr, "  m use 3.6       # Verify MongoDB 3.6 is available\n")
	fmt.Fprintf(os.Stderr, "  m bin 3.6       # Output bin directory path for MongoDB 3.6\n")
	fmt.Fprintf(os.Stderr, "  m implode       # Remove all installed versions and clean up\n\n")
}
