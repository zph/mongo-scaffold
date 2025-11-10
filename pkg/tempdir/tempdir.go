package tempdir

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// Manager handles creation and cleanup of temporary directories for cluster data
type Manager struct {
	baseDir    string
	cleanupReg *CleanupRegistry
}

// Metadata contains information about the cluster stored in the temp directory
type Metadata struct {
	Ports       []int  `json:"ports"`
	PIDs        []int  `json:"pids"`
	Version     string `json:"version"`
	CreatedAt   string `json:"created_at"`
	MongosHosts []string `json:"mongos_hosts"`
}

// CleanupRegistry tracks temp directories for cleanup on exit
type CleanupRegistry struct {
	mu   sync.Mutex
	dirs []string
	once sync.Once
}

var globalRegistry = &CleanupRegistry{
	dirs: make([]string, 0),
}

// NewManager creates a new temp directory manager with automatic cleanup
func NewManager() (*Manager, error) {
	// Create unique temp directory
	baseDir, err := os.MkdirTemp("", "mongo-test-harness-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Register for cleanup on exit
	globalRegistry.Register(baseDir)

	return &Manager{
		baseDir:    baseDir,
		cleanupReg: globalRegistry,
	}, nil
}

// CreateClusterDir creates the necessary subdirectories for cluster data
func (m *Manager) CreateClusterDir() error {
	dirs := []string{"data", "logs"}
	for _, dir := range dirs {
		path := filepath.Join(m.baseDir, dir)
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}
	return nil
}

// BaseDir returns the base temporary directory path
func (m *Manager) BaseDir() string {
	return m.baseDir
}

// DataDir returns the path to the data directory
func (m *Manager) DataDir() string {
	return filepath.Join(m.baseDir, "data")
}

// LogDir returns the path to the logs directory
func (m *Manager) LogDir() string {
	return filepath.Join(m.baseDir, "logs")
}

// MetadataPath returns the path to the metadata file
func (m *Manager) MetadataPath() string {
	return filepath.Join(m.baseDir, "metadata.json")
}

// WriteMetadata writes cluster metadata to the temp directory
func (m *Manager) WriteMetadata(meta Metadata) error {
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(m.MetadataPath(), data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// ReadMetadata reads cluster metadata from the temp directory
func (m *Manager) ReadMetadata() (*Metadata, error) {
	data, err := os.ReadFile(m.MetadataPath())
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &meta, nil
}

// Cleanup removes the temporary directory and all its contents
func (m *Manager) Cleanup() error {
	if m.baseDir == "" {
		return nil
	}

	log.Printf("Cleaning up temp directory: %s", m.baseDir)

	// Remove from cleanup registry
	m.cleanupReg.Unregister(m.baseDir)

	// Force remove with retries (Windows can be finicky)
	return removeAllWithRetry(m.baseDir, 3)
}

// Register adds a directory to the cleanup registry
func (r *CleanupRegistry) Register(dir string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.dirs = append(r.dirs, dir)

	// Setup exit handler on first registration
	r.once.Do(func() {
		registerExitHandler(r)
	})
}

// Unregister removes a directory from the cleanup registry
func (r *CleanupRegistry) Unregister(dir string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, d := range r.dirs {
		if d == dir {
			r.dirs = append(r.dirs[:i], r.dirs[i+1:]...)
			break
		}
	}
}

// CleanupAll removes all registered directories
func (r *CleanupRegistry) CleanupAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, dir := range r.dirs {
		if err := removeAllWithRetry(dir, 3); err != nil {
			log.Printf("Failed to cleanup %s: %v", dir, err)
		}
	}

	r.dirs = nil
}

// registerExitHandler sets up signal handlers for cleanup
func registerExitHandler(reg *CleanupRegistry) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("Received shutdown signal, cleaning up temp directories...")
		reg.CleanupAll()
		os.Exit(1)
	}()
}

// removeAllWithRetry attempts to remove a directory with retries
func removeAllWithRetry(path string, retries int) error {
	var lastErr error
	for i := 0; i < retries; i++ {
		err := os.RemoveAll(path)
		if err == nil {
			return nil
		}
		lastErr = err
		time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
	}
	return fmt.Errorf("failed to remove %s after %d attempts: %w", path, retries, lastErr)
}
