package mlaunch

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
)

const (
	mtoolsVersion = "1.7.0"
	pythonVersion = "3.10"
)

// Installer handles installation of uv and mtools
type Installer struct {
	uvPath      string
	mlaunchPath string
}

// NewInstaller creates a new installer instance
func NewInstaller() *Installer {
	return &Installer{}
}

// EnsureDependencies ensures uv and mtools are installed
func (i *Installer) EnsureDependencies() error {
	// Check if uv is installed
	if err := i.ensureUV(); err != nil {
		return fmt.Errorf("failed to ensure uv is installed: %w", err)
	}

	// Check if mlaunch is installed
	if err := i.ensureMtools(); err != nil {
		return fmt.Errorf("failed to ensure mtools is installed: %w", err)
	}

	return nil
}

// ensureUV checks if uv is installed and installs it if necessary
func (i *Installer) ensureUV() error {
	// Check if uv is already in PATH
	uvPath, err := exec.LookPath("uv")
	if err == nil {
		i.uvPath = uvPath
		return nil
	}

	fmt.Println("uv not found, installing...")

	// Install uv using the official installer
	if err := i.installUV(); err != nil {
		return fmt.Errorf("failed to install uv: %w", err)
	}

	// Try to find uv again
	uvPath, err = exec.LookPath("uv")
	if err != nil {
		return fmt.Errorf("uv installed but not found in PATH: %w", err)
	}

	i.uvPath = uvPath
	return nil
}

// installUV installs uv using the official installation method
func (i *Installer) installUV() error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin", "linux":
		// Use curl to download and execute the installer
		cmd = exec.Command("sh", "-c", "curl -LsSf https://astral.sh/uv/install.sh | sh")
	case "windows":
		// Use PowerShell to download and execute the installer
		cmd = exec.Command("powershell", "-c", "irm https://astral.sh/uv/install.ps1 | iex")
	default:
		return fmt.Errorf("unsupported operating system: %s", runtime.GOOS)
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to run uv installer: %w", err)
	}

	return nil
}

// ensureMtools checks if mtools is installed and installs it if necessary
func (i *Installer) ensureMtools() error {
	// Check if mlaunch is available
	mlaunchPath, err := exec.LookPath("mlaunch")
	if err == nil {
		i.mlaunchPath = mlaunchPath
		return nil
	}

	fmt.Println("mlaunch not found, installing mtools...")

	// Install mtools using uv tool
	if err := i.installMtools(); err != nil {
		return fmt.Errorf("failed to install mtools: %w", err)
	}

	// Try to find mlaunch again
	mlaunchPath, err = exec.LookPath("mlaunch")
	if err != nil {
		return fmt.Errorf("mlaunch installed but not found in PATH: %w", err)
	}

	i.mlaunchPath = mlaunchPath
	return nil
}

// installMtools installs mtools using uv tool install
func (i *Installer) installMtools() error {
	if i.uvPath == "" {
		return fmt.Errorf("uv is not installed")
	}

	// Build the command: uv tool install --python 3.10 mtools[mlaunch]==1.7.0 --with pymongo==3.13.0 --with python-dateutil
	args := []string{
		"tool",
		"install",
		"--python", pythonVersion,
		fmt.Sprintf("mtools[mlaunch]==%s", mtoolsVersion),
		"--with", "pymongo==3.13.0",
		"--with", "python-dateutil",
	}

	cmd := exec.Command(i.uvPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to install mtools: %w", err)
	}

	return nil
}

// GetMlaunchPath returns the path to the mlaunch binary
func (i *Installer) GetMlaunchPath() string {
	return i.mlaunchPath
}

// GetUVPath returns the path to the uv binary
func (i *Installer) GetUVPath() string {
	return i.uvPath
}
