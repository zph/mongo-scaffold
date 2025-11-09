package mwrapper

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// Manager handles MongoDB version management using the 'm' tool
type Manager struct {
	mPath string
}

// NewManager creates a new m version manager wrapper
// It checks if 'm' is available in PATH
func NewManager() (*Manager, error) {
	mPath, err := exec.LookPath("m")
	if err != nil {
		return nil, fmt.Errorf("m binary not found in PATH: %w", err)
	}

	return &Manager{
		mPath: mPath,
	}, nil
}

// EnsureVersion ensures the specified MongoDB version is installed and available
// Returns the path to the mongod binary for that version
func (m *Manager) EnsureVersion(version string) (string, error) {
	// Check if version is already installed
	installed, err := m.IsVersionInstalled(version)
	if err != nil {
		return "", fmt.Errorf("failed to check if version is installed: %w", err)
	}

	if !installed {
		// Install the version
		if err := m.InstallVersion(version); err != nil {
			return "", fmt.Errorf("failed to install MongoDB %s: %w", version, err)
		}
	}

	// Get the path to the binary
	return m.GetVersionPath(version)
}

// IsVersionInstalled checks if a MongoDB version is installed
func (m *Manager) IsVersionInstalled(version string) (bool, error) {
	versions, err := m.ListInstalledVersions()
	if err != nil {
		return false, err
	}

	for _, v := range versions {
		if v == version {
			return true, nil
		}
	}

	return false, nil
}

// InstallVersion installs a specific MongoDB version using m
func (m *Manager) InstallVersion(version string) error {
	cmd := exec.Command(m.mPath, version)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install MongoDB %s: %w", version, err)
	}

	return nil
}

// ListInstalledVersions returns a list of installed MongoDB versions
func (m *Manager) ListInstalledVersions() ([]string, error) {
	cmd := exec.Command(m.mPath, "ls")
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to list versions: %w", err)
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	versions := make([]string, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			// Remove any leading markers like "*" for active version
			line = strings.TrimPrefix(line, "* ")
			versions = append(versions, line)
		}
	}

	return versions, nil
}

// GetVersionPath returns the path to the mongod binary for a specific version
func (m *Manager) GetVersionPath(version string) (string, error) {
	// m stores versions in ~/.m/versions/{version}/bin/mongod
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	binaryName := "mongod"
	if runtime.GOOS == "windows" {
		binaryName = "mongod.exe"
	}

	mongodPath := filepath.Join(homeDir, ".m", "versions", version, "bin", binaryName)

	// Verify the binary exists
	if _, err := os.Stat(mongodPath); err != nil {
		return "", fmt.Errorf("mongod binary not found at %s: %w", mongodPath, err)
	}

	return mongodPath, nil
}

// GetBinPath returns the bin directory path for a specific version
func (m *Manager) GetBinPath(version string) (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}

	binPath := filepath.Join(homeDir, ".m", "versions", version, "bin")

	// Verify the directory exists
	if _, err := os.Stat(binPath); err != nil {
		return "", fmt.Errorf("bin directory not found at %s: %w", binPath, err)
	}

	return binPath, nil
}

// UseVersion sets the specified version as active (adds to PATH)
// This is useful if you want to use mongo shell or other utilities
func (m *Manager) UseVersion(version string) error {
	cmd := exec.Command(m.mPath, "use", version)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to use MongoDB %s: %w", version, err)
	}

	return nil
}
