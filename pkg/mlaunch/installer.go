package mlaunch

import (
	"fmt"
	"os/exec"
)

// Installer handles installation of mtools (kept for backward compatibility)
type Installer struct {
	mlaunchPath string
}

// NewInstaller creates a new installer instance
func NewInstaller() *Installer {
	return &Installer{}
}

// EnsureDependencies ensures mtools is installed (kept for backward compatibility)
// Note: The native Go implementation doesn't require mtools, so this is a no-op
func (i *Installer) EnsureDependencies() error {
	// Check if mlaunch is installed (for backward compatibility)
	if err := i.ensureMtools(); err != nil {
		return fmt.Errorf("failed to ensure mtools is installed: %w", err)
	}

	return nil
}

// ensureMtools checks if mtools is installed (kept for backward compatibility)
// Note: The native Go implementation doesn't require mtools, so this is a no-op
func (i *Installer) ensureMtools() error {
	// Check if mlaunch is available (for backward compatibility)
	mlaunchPath, err := exec.LookPath("mlaunch")
	if err == nil {
		i.mlaunchPath = mlaunchPath
		return nil
	}

	// mtools is not required for the native Go implementation
	// Return nil to allow the code to continue without mtools
	return nil
}

// GetMlaunchPath returns the path to the mlaunch binary (if available)
func (i *Installer) GetMlaunchPath() string {
	return i.mlaunchPath
}
