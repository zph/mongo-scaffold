package mongo_version_manager

import (
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"

	"golang.org/x/mod/semver"
)

// Manager handles MongoDB version management using native Go implementation
type Manager struct {
	tempDirs   []string
	tempMu     sync.Mutex
	installDir string // Base directory for MongoDB installations (e.g., ~/.m/versions)
}

// NewManager creates a new MongoDB version manager
func NewManager() (*Manager, error) {
	var installDir string

	// Check for custom install directory via environment variable (for testing)
	if envDir := os.Getenv("M_INSTALL_DIR"); envDir != "" {
		// Use environment variable if set
		absDir, err := filepath.Abs(envDir)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve M_INSTALL_DIR path: %w", err)
		}
		installDir = absDir
	} else {
		// Default install directory: ~/.m/versions (same as m tool)
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("failed to get home directory: %w", err)
		}
		installDir = filepath.Join(homeDir, ".m", "versions")
	}

	return &Manager{
		tempDirs:   make([]string, 0),
		installDir: installDir,
	}, nil
}

// EnsureVersion ensures the specified MongoDB version is installed and available
// Returns the path to the mongod binary for that version
func (m *Manager) EnsureVersion(version string) (string, error) {
	binPath, err := m.GetBinPath(version)
	if err != nil {
		return "", err
	}

	binaryName := "mongod"
	if runtime.GOOS == "windows" {
		binaryName = "mongod.exe"
	}

	return filepath.Join(binPath, binaryName), nil
}

// normalizeVersion adds "v" prefix if needed for semver library
func normalizeVersion(version string) string {
	if !strings.HasPrefix(version, "v") {
		return "v" + version
	}
	return version
}

// versionMatches checks if installedVersion matches requiredVersion using semver
// If requiredVersion is a patch version (X.Y.Z), requires exact match
// If requiredVersion is a minor version (X.Y), matches any patch version with same major.minor
func versionMatches(installedVersion, requiredVersion string) bool {
	installedSemver := normalizeVersion(installedVersion)
	requiredSemver := normalizeVersion(requiredVersion)

	// Check if versions are valid semver
	if !semver.IsValid(installedSemver) || !semver.IsValid(requiredSemver) {
		// Fallback to string comparison if not valid semver
		return installedVersion == requiredVersion
	}

	// If required version is a patch version (has 3 parts), require exact match
	requiredParts := strings.Split(strings.TrimPrefix(requiredSemver, "v"), ".")
	if len(requiredParts) >= 3 {
		// Exact match required
		return semver.Compare(installedSemver, requiredSemver) == 0
	}

	// If required version is minor (X.Y), check if major.minor matches
	requiredMajorMinor := semver.MajorMinor(requiredSemver)
	installedMajorMinor := semver.MajorMinor(installedSemver)
	return requiredMajorMinor == installedMajorMinor
}

// checkMongoVersion checks if mongod is on PATH and returns its version
func (m *Manager) checkMongoVersion(requiredVersion string) (string, bool, error) {
	mongodPath, err := exec.LookPath("mongod")
	if err != nil {
		return "", false, nil // Not on PATH
	}

	// Get version from mongod
	cmd := exec.Command(mongodPath, "--version")
	output, err := cmd.Output()
	if err != nil {
		return "", false, nil // Can't get version
	}

	// Parse version from output (e.g., "db version v3.6.23")
	versionRegex := regexp.MustCompile(`db version v(\d+\.\d+\.\d+)`)
	matches := versionRegex.FindStringSubmatch(string(output))
	if len(matches) < 2 {
		return "", false, nil // Can't parse version
	}

	installedVersion := matches[1]

	// Check version match using semver
	if versionMatches(installedVersion, requiredVersion) {
		return filepath.Dir(mongodPath), true, nil
	}

	return "", false, nil // Version doesn't match
}

// InstallVersion installs a specific MongoDB version to a temp directory
func (m *Manager) InstallVersion(version string) (string, error) {
	// Create temp directory for this version
	tempDir, err := os.MkdirTemp("", fmt.Sprintf("mongodb-%s-*", version))
	if err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Track temp directory for cleanup
	m.tempMu.Lock()
	m.tempDirs = append(m.tempDirs, tempDir)
	m.tempMu.Unlock()

	fmt.Printf("Installing MongoDB %s to %s...\n", version, tempDir)

	// Get download URL (getDownloadURL handles ARM64 -> x86_64 fallback automatically)
	url, err := m.getDownloadURL(version, runtime.GOARCH)
	if err != nil {
		return "", fmt.Errorf("failed to get download URL: %w", err)
	}

	fmt.Printf("Downloading from %s...\n", url)

	// Download the archive
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to download MongoDB: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download MongoDB: HTTP %d", resp.StatusCode)
	}

	// Create temporary file for the archive
	tmpFile, err := os.CreateTemp("", "mongodb-*.tgz")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Download to temp file
	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return "", fmt.Errorf("failed to download MongoDB: %w", err)
	}

	// Reset file pointer
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return "", fmt.Errorf("failed to seek temp file: %w", err)
	}

	fmt.Println("Extracting...")

	// Extract the archive to temp directory (will extract to tempDir/mongodb-...)
	extractDir := tempDir
	if err := m.extractArchive(tmpFile, extractDir); err != nil {
		return "", fmt.Errorf("failed to extract MongoDB: %w", err)
	}

	// Find the extracted bin directory
	// The archive may extract in two ways:
	// 1. With root directory: tempDir/mongodb-{os}-{arch}-{version}/bin
	// 2. Without root (stripped): tempDir/bin (directly)
	entries, err := os.ReadDir(extractDir)
	if err != nil {
		return "", fmt.Errorf("failed to read extract directory: %w", err)
	}

	var binPath string

	// First, check if bin directory is directly in extractDir (stripped root)
	directBinPath := filepath.Join(extractDir, "bin")
	if _, err := os.Stat(directBinPath); err == nil {
		// Verify it contains mongod
		mongodPath := filepath.Join(directBinPath, "mongod")
		if runtime.GOOS == "windows" {
			mongodPath = filepath.Join(directBinPath, "mongod.exe")
		}
		if _, err := os.Stat(mongodPath); err == nil {
			binPath = directBinPath
		}
	}

	// If not found, look for mongodb-* directories
	if binPath == "" {
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "mongodb-") {
				potentialBinPath := filepath.Join(extractDir, entry.Name(), "bin")
				if _, err := os.Stat(potentialBinPath); err == nil {
					binPath = potentialBinPath
					break
				}
			}
		}
	}

	// If still not found, try searching recursively
	if binPath == "" {
		err := filepath.Walk(extractDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil // Continue searching
			}
			// Check if this is a bin directory containing mongod
			if info.IsDir() && info.Name() == "bin" {
				mongodPath := filepath.Join(path, "mongod")
				if runtime.GOOS == "windows" {
					mongodPath = filepath.Join(path, "mongod.exe")
				}
				if _, err := os.Stat(mongodPath); err == nil {
					binPath = path
					return filepath.SkipAll // Found it, stop searching
				}
			}
			return nil
		})
		if err != nil {
			return "", fmt.Errorf("failed to search for bin directory: %w", err)
		}
	}

	if binPath == "" {
		return "", fmt.Errorf("failed to find bin directory in extracted archive")
	}

	// Resolve the version to get the exact patch version (e.g., "4.0" -> "4.0.28")
	resolvedVersion, err := m.resolveVersion(version)
	if err != nil {
		return "", fmt.Errorf("failed to resolve version: %w", err)
	}

	// Copy all executables to the permanent installation directory
	// Format: ~/.m/versions/{resolvedVersion}/bin
	versionInstallDir := filepath.Join(m.installDir, resolvedVersion)
	versionBinDir := filepath.Join(versionInstallDir, "bin")

	if err := os.MkdirAll(versionBinDir, 0755); err != nil {
		return "", fmt.Errorf("failed to create installation directory: %w", err)
	}

	fmt.Printf("Copying binaries to %s...\n", versionBinDir)
	if err := m.copyBinaries(binPath, versionBinDir); err != nil {
		return "", fmt.Errorf("failed to copy binaries: %w", err)
	}

	// For MongoDB 4.0+, automatically install mongosh if it's not already present
	// mongosh is compatible with MongoDB servers >= 4.0 and is not included in the server package
	if err := m.ensureMongosh(resolvedVersion, versionBinDir); err != nil {
		// Log warning but don't fail installation - mongosh is optional
		fmt.Printf("Warning: failed to install mongosh: %v\n", err)
		fmt.Printf("You can manually install mongosh from https://www.mongodb.com/try/download/shell\n")
	}

	fmt.Printf("MongoDB %s installed successfully to %s\n", version, versionBinDir)
	return versionBinDir, nil
}

// ensureMongosh ensures mongosh is installed for MongoDB 4.0+ versions
// mongosh is compatible with MongoDB servers >= 4.0
// It installs the latest mongosh from GitHub releases in the same bin directory as the server binaries
func (m *Manager) ensureMongosh(version string, binDir string) error {
	// Check if version is 4.0+
	versionSemver := normalizeVersion(version)
	parts := strings.Split(strings.TrimPrefix(versionSemver, "v"), ".")
	if len(parts) >= 2 {
		// Ensure we have at least major.minor.patch format
		if len(parts) == 2 {
			versionSemver = "v" + strings.Join(parts, ".") + ".0"
		}
		// Only install mongosh for MongoDB 4.0+ (mongosh compatibility requirement)
		if semver.Compare(versionSemver, "v4.0.0") < 0 {
			return nil // Not needed for versions < 4.0
		}
	} else {
		return nil // Can't determine version, skip
	}

	// Check if mongosh already exists
	mongoshPath := filepath.Join(binDir, "mongosh")
	if runtime.GOOS == "windows" {
		mongoshPath += ".exe"
	}
	if _, err := os.Stat(mongoshPath); err == nil {
		// mongosh already exists, nothing to do
		return nil
	}

	// Download and install latest mongosh from GitHub releases
	fmt.Printf("Installing latest mongosh for MongoDB %s...\n", version)
	return m.installMongoshFromGitHub(binDir)
}

// installMongoshFromGitHub downloads and installs the latest mongosh from GitHub releases
// It places the mongosh binary in the specified bin directory
func (m *Manager) installMongoshFromGitHub(binDir string) error {
	// Get the latest mongosh release from GitHub
	releaseURL, err := m.getLatestMongoshReleaseURL()
	if err != nil {
		return fmt.Errorf("failed to get latest mongosh release: %w", err)
	}

	fmt.Printf("Downloading mongosh from %s...\n", releaseURL)

	// Download the archive
	resp, err := http.Get(releaseURL)
	if err != nil {
		return fmt.Errorf("failed to download mongosh: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to download mongosh: HTTP %d", resp.StatusCode)
	}

	// Determine file extension based on OS
	var fileExt string
	if runtime.GOOS == "darwin" {
		fileExt = ".zip"
	} else {
		fileExt = ".tgz"
	}

	// Create temporary file for the archive
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("mongosh-*%s", fileExt))
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Download to temp file
	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		return fmt.Errorf("failed to download mongosh: %w", err)
	}

	// Reset file pointer
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek temp file: %w", err)
	}

	// Create temp directory for extraction
	tempDir, err := os.MkdirTemp("", "mongosh-extract-*")
	if err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}
	defer os.RemoveAll(tempDir)

	fmt.Println("Extracting mongosh...")

	// Extract the archive (zip for darwin, tgz for linux)
	if runtime.GOOS == "darwin" {
		if err := m.extractZipArchive(tmpFile, tempDir); err != nil {
			return fmt.Errorf("failed to extract mongosh: %w", err)
		}
	} else {
		if err := m.extractArchive(tmpFile, tempDir); err != nil {
			return fmt.Errorf("failed to extract mongosh: %w", err)
		}
	}

	// Find the mongosh binary in the extracted archive
	// mongosh archives typically have structure: mongosh-{version}-{os}-{arch}/bin/mongosh
	var mongoshBinaryPath string
	err = filepath.Walk(tempDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Continue searching
		}
		// Look for mongosh binary
		if !info.IsDir() {
			name := info.Name()
			if name == "mongosh" || (runtime.GOOS == "windows" && name == "mongosh.exe") {
				mongoshBinaryPath = path
				return filepath.SkipAll // Found it, stop searching
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to search for mongosh binary: %w", err)
	}

	if mongoshBinaryPath == "" {
		return fmt.Errorf("failed to find mongosh binary in extracted archive")
	}

	// Copy mongosh binary to bin directory
	targetPath := filepath.Join(binDir, filepath.Base(mongoshBinaryPath))
	sourceFile, err := os.Open(mongoshBinaryPath)
	if err != nil {
		return fmt.Errorf("failed to open mongosh binary: %w", err)
	}
	defer sourceFile.Close()

	info, err := sourceFile.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat mongosh binary: %w", err)
	}

	targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return fmt.Errorf("failed to create target mongosh binary: %w", err)
	}
	defer targetFile.Close()

	if _, err := io.Copy(targetFile, sourceFile); err != nil {
		return fmt.Errorf("failed to copy mongosh binary: %w", err)
	}

	// Preserve executable permissions
	if err := os.Chmod(targetPath, info.Mode()); err != nil {
		return fmt.Errorf("failed to set permissions on mongosh binary: %w", err)
	}

	fmt.Printf("mongosh installed successfully to %s\n", targetPath)
	return nil
}

// GitHubRelease represents a GitHub release
type GitHubRelease struct {
	TagName string        `json:"tag_name"`
	Assets  []GitHubAsset `json:"assets"`
}

// GitHubAsset represents a GitHub release asset
type GitHubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

// getLatestMongoshReleaseURL gets the download URL for the latest mongosh release from GitHub
// Returns the URL for the platform-appropriate binary (darwin/linux, x64/arm64)
func (m *Manager) getLatestMongoshReleaseURL() (string, error) {
	// Fetch latest release from GitHub API
	resp, err := http.Get("https://api.github.com/repos/mongodb-js/mongosh/releases/latest")
	if err != nil {
		return "", fmt.Errorf("failed to fetch mongosh releases: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch mongosh releases: HTTP %d", resp.StatusCode)
	}

	var release GitHubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return "", fmt.Errorf("failed to parse mongosh releases: %w", err)
	}

	// Determine platform-specific asset name pattern
	// darwin uses .zip, linux uses .tgz
	var fileExt string
	var archName string
	switch runtime.GOOS {
	case "darwin":
		fileExt = ".zip"
		if runtime.GOARCH == "arm64" {
			archName = "arm64"
		} else {
			archName = "x64"
		}
	case "linux":
		fileExt = ".tgz"
		if runtime.GOARCH == "arm64" {
			archName = "arm64"
		} else {
			archName = "x64"
		}
	default:
		return "", fmt.Errorf("unsupported OS for mongosh: %s", runtime.GOOS)
	}

	// Find matching asset
	version := strings.TrimPrefix(release.TagName, "v")
	expectedName := fmt.Sprintf("mongosh-%s-%s-%s%s", version, runtime.GOOS, archName, fileExt)

	for _, asset := range release.Assets {
		if asset.Name == expectedName {
			return asset.BrowserDownloadURL, nil
		}
	}

	// If exact match not found, try to find any matching platform asset
	// (in case version format differs slightly)
	platformPattern := fmt.Sprintf("%s-%s%s", runtime.GOOS, archName, fileExt)

	for _, asset := range release.Assets {
		if strings.Contains(asset.Name, platformPattern) && strings.HasSuffix(asset.Name, fileExt) {
			// Skip signature files
			if strings.HasSuffix(asset.Name, ".sig") {
				continue
			}
			return asset.BrowserDownloadURL, nil
		}
	}

	return "", fmt.Errorf("failed to find platform-appropriate mongosh asset for %s/%s in release %s", runtime.GOOS, runtime.GOARCH, release.TagName)
}

// copyBinaries copies all executable files from source bin directory to target bin directory
func (m *Manager) copyBinaries(sourceBinDir, targetBinDir string) error {
	entries, err := os.ReadDir(sourceBinDir)
	if err != nil {
		return fmt.Errorf("failed to read source bin directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Check if it's an executable file
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// On Unix systems, check if file is executable
		// On Windows, copy all files (they might be .exe)
		isExecutable := false
		if runtime.GOOS == "windows" {
			// On Windows, copy .exe, .bat, .cmd files
			name := entry.Name()
			isExecutable = strings.HasSuffix(strings.ToLower(name), ".exe") ||
				strings.HasSuffix(strings.ToLower(name), ".bat") ||
				strings.HasSuffix(strings.ToLower(name), ".cmd")
		} else {
			// On Unix, check if file has execute permission
			isExecutable = info.Mode().Perm()&0111 != 0
		}

		if !isExecutable {
			continue
		}

		sourcePath := filepath.Join(sourceBinDir, entry.Name())
		targetPath := filepath.Join(targetBinDir, entry.Name())

		// Copy the file
		sourceFile, err := os.Open(sourcePath)
		if err != nil {
			return fmt.Errorf("failed to open source file %s: %w", sourcePath, err)
		}
		defer sourceFile.Close()

		targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
		if err != nil {
			return fmt.Errorf("failed to create target file %s: %w", targetPath, err)
		}
		defer targetFile.Close()

		if _, err := io.Copy(targetFile, sourceFile); err != nil {
			return fmt.Errorf("failed to copy file %s to %s: %w", sourcePath, targetPath, err)
		}

		// Preserve executable permissions
		if err := os.Chmod(targetPath, info.Mode()); err != nil {
			return fmt.Errorf("failed to set permissions on %s: %w", targetPath, err)
		}
	}

	return nil
}

// MongoDBFullJSON represents the full.json structure
type MongoDBFullJSON struct {
	Versions []MongoDBVersionInfo `json:"versions"`
}

// MongoDBVersionInfo represents version information from full.json
type MongoDBVersionInfo struct {
	Version   string            `json:"version"`
	Downloads []MongoDBDownload `json:"downloads"`
}

// MongoDBDownload represents a download entry in full.json
type MongoDBDownload struct {
	Arch    string         `json:"arch"`
	Target  string         `json:"target"`
	Archive MongoDBArchive `json:"archive"`
	Edition string         `json:"edition"`
}

// MongoDBArchive represents archive information
type MongoDBArchive struct {
	URL string `json:"url"`
}

// getDownloadURL fetches the MongoDB full.json and finds the correct download URL
// for the given version, OS, and architecture, with fallback to x86_64 if ARM64 not available
func (m *Manager) getDownloadURL(version string, goArch string) (string, error) {
	// Fetch full.json from MongoDB
	resp, err := http.Get("https://downloads.mongodb.org/full.json")
	if err != nil {
		return "", fmt.Errorf("failed to fetch MongoDB versions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch MongoDB versions: HTTP %d", resp.StatusCode)
	}

	var fullJSON MongoDBFullJSON
	if err := json.NewDecoder(resp.Body).Decode(&fullJSON); err != nil {
		return "", fmt.Errorf("failed to parse MongoDB versions: %w", err)
	}
	versions := fullJSON.Versions

	// Map Go architecture to MongoDB architecture names
	var targetArchs []string
	if goArch == "arm64" {
		// Try ARM64 first, then fallback to x86_64
		targetArchs = []string{"arm64", "x86_64"}
	} else {
		targetArchs = []string{"x86_64"}
	}

	// Map Go OS to MongoDB target names
	var targetOS string
	switch runtime.GOOS {
	case "darwin":
		targetOS = "macos"
	case "linux":
		// For Linux, we'll match any Linux target (ubuntu, rhel, debian, etc.)
		// We'll check the URL to ensure it's a Linux download
		targetOS = "linux"
	default:
		return "", fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}

	// Determine if user specified a patch version (X.Y.Z) or minor version (X.Y)
	versionParts := strings.Split(version, ".")
	if len(versionParts) < 2 {
		return "", fmt.Errorf("invalid version format: %s (expected X.Y or X.Y.Z)", version)
	}

	var targetVersion MongoDBVersionInfo
	var foundExactMatch bool

	// Check if it's a patch version (X.Y.Z)
	if len(versionParts) >= 3 {
		// User specified exact patch version (e.g., "4.0.28")
		exactVersion := version
		for _, v := range versions {
			// Skip RC and pre-release versions unless explicitly requested
			if strings.Contains(strings.ToLower(v.Version), "rc") ||
				strings.Contains(strings.ToLower(v.Version), "alpha") ||
				strings.Contains(strings.ToLower(v.Version), "beta") {
				// Only skip if the requested version doesn't contain these
				if !strings.Contains(strings.ToLower(exactVersion), "rc") &&
					!strings.Contains(strings.ToLower(exactVersion), "alpha") &&
					!strings.Contains(strings.ToLower(exactVersion), "beta") {
					continue
				}
			}

			if v.Version == exactVersion {
				targetVersion = v
				foundExactMatch = true
				break
			}
		}

		if !foundExactMatch {
			return "", fmt.Errorf("exact version %s not found", exactVersion)
		}
	} else {
		// User specified minor version (e.g., "4.0") - find latest patch
		majorMinor := versionParts[0] + "." + versionParts[1]

		// Collect all matching versions and find the latest patch version
		var matchingVersions []MongoDBVersionInfo
		for _, v := range versions {
			// Skip RC and pre-release versions
			if strings.Contains(strings.ToLower(v.Version), "rc") ||
				strings.Contains(strings.ToLower(v.Version), "alpha") ||
				strings.Contains(strings.ToLower(v.Version), "beta") {
				continue
			}

			if strings.HasPrefix(v.Version, majorMinor+".") {
				matchingVersions = append(matchingVersions, v)
			}
		}

		if len(matchingVersions) == 0 {
			return "", fmt.Errorf("no versions found for MongoDB %s", majorMinor)
		}

		// Use semantic version comparison to find the latest version
		targetVersion = matchingVersions[0]
		targetVersionSemver := "v" + targetVersion.Version

		for _, v := range matchingVersions[1:] {
			currentSemver := "v" + v.Version
			// Compare using semver.Compare (returns -1 if current < latest, 0 if equal, 1 if current > latest)
			if semver.Compare(currentSemver, targetVersionSemver) > 0 {
				targetVersion = v
				targetVersionSemver = currentSemver
			}
		}
	}

	latestVersion := targetVersion

	// Try each architecture in order
	// Look for community edition (base/targeted) first, then enterprise
	for _, targetArch := range targetArchs {
		// First try community edition (base or targeted), then enterprise
		// But if no edition preference, accept any edition
		editions := []string{"base", "targeted", "enterprise", ""}
		for _, edition := range editions {
			for _, download := range latestVersion.Downloads {
				// For macOS, match target exactly or accept "osx" and "osx-ssl" (older format)
				// For Linux, match any Linux target (check URL contains "linux")
				matchesTarget := false
				if runtime.GOOS == "darwin" {
					matchesTarget = download.Target == targetOS ||
						download.Target == "osx" ||
						download.Target == "osx-ssl" ||
						strings.Contains(strings.ToLower(download.Target), "macos") ||
						strings.Contains(strings.ToLower(download.Target), "osx")
				} else if runtime.GOOS == "linux" {
					// For Linux, accept any Linux target or check URL
					matchesTarget = strings.Contains(strings.ToLower(download.Target), "linux") ||
						strings.Contains(strings.ToLower(download.Target), "ubuntu") ||
						strings.Contains(strings.ToLower(download.Target), "rhel") ||
						strings.Contains(strings.ToLower(download.Target), "debian") ||
						strings.Contains(strings.ToLower(download.Archive.URL), "linux")
				}

				if download.Arch == targetArch && matchesTarget {
					// If edition is empty, accept any edition
					// Otherwise, prefer community edition (base/targeted) over enterprise
					editionMatches := edition == "" ||
						download.Edition == edition ||
						(edition == "base" && download.Edition == "targeted") ||
						(edition == "targeted" && download.Edition == "base")
					if editionMatches {
						if download.Archive.URL != "" {
							return download.Archive.URL, nil
						}
					}
				}
			}
		}
	}

	// Fallback: If no download found in full.json, try constructing URL directly
	// This is needed for older versions that might not be in full.json
	return m.constructFallbackURL(latestVersion.Version, targetOS, targetArchs)
}

// constructFallbackURL constructs a download URL directly for older versions
// that might not be in full.json
func (m *Manager) constructFallbackURL(version, targetOS string, targetArchs []string) (string, error) {
	// Try each architecture in order
	for _, targetArch := range targetArchs {
		// Try multiple URL patterns for older versions
		var urls []string

		if targetOS == "macos" {
			// For older MongoDB versions, try different URL patterns
			// Pattern 1: mongodb-osx-x86_64-{version}.tgz (older format)
			// Pattern 2: mongodb-macos-x86_64-{version}.tgz (newer format)
			if targetArch == "x86_64" {
				urls = []string{
					fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-osx-x86_64-%s.tgz", version),
					fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-macos-x86_64-%s.tgz", version),
				}
			} else if targetArch == "arm64" {
				urls = []string{
					fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-macos-arm64-%s.tgz", version),
					fmt.Sprintf("https://fastdl.mongodb.org/osx/mongodb-osx-arm64-%s.tgz", version),
				}
			}
		} else if targetOS == "linux" {
			urls = []string{
				fmt.Sprintf("https://fastdl.mongodb.org/linux/mongodb-linux-%s-%s.tgz", targetArch, version),
			}
		}

		// Try each URL pattern
		for _, url := range urls {
			// Verify the URL exists by making a HEAD request
			resp, err := http.Head(url)
			if err == nil {
				statusCode := resp.StatusCode
				resp.Body.Close()
				// Accept 200 OK or 301/302 redirects (some servers redirect)
				if statusCode == http.StatusOK || statusCode == http.StatusMovedPermanently || statusCode == http.StatusFound {
					if statusCode != http.StatusOK {
						// Follow redirect
						redirectURL := resp.Header.Get("Location")
						if redirectURL != "" {
							url = redirectURL
						}
					}
					return url, nil
				}
			}
		}
	}

	return "", fmt.Errorf("no download URL found for MongoDB %s on %s", version, targetOS)
}

// resolveVersion resolves a version string to the exact patch version
// If user specified patch version (X.Y.Z), returns it as-is
// If user specified minor version (X.Y), finds and returns the latest patch version
func (m *Manager) resolveVersion(version string) (string, error) {
	// Fetch full.json from MongoDB
	resp, err := http.Get("https://downloads.mongodb.org/full.json")
	if err != nil {
		return "", fmt.Errorf("failed to fetch MongoDB versions: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to fetch MongoDB versions: HTTP %d", resp.StatusCode)
	}

	var fullJSON MongoDBFullJSON
	if err := json.NewDecoder(resp.Body).Decode(&fullJSON); err != nil {
		return "", fmt.Errorf("failed to parse MongoDB versions: %w", err)
	}
	versions := fullJSON.Versions

	// Determine if user specified a patch version (X.Y.Z) or minor version (X.Y)
	versionParts := strings.Split(version, ".")
	if len(versionParts) < 2 {
		return "", fmt.Errorf("invalid version format: %s (expected X.Y or X.Y.Z)", version)
	}

	// If it's a patch version (X.Y.Z), return as-is
	if len(versionParts) >= 3 {
		// Verify the exact version exists
		exactVersion := version
		for _, v := range versions {
			if v.Version == exactVersion {
				return exactVersion, nil
			}
		}
		return "", fmt.Errorf("exact version %s not found", exactVersion)
	}

	// User specified minor version (e.g., "4.0") - find latest patch
	majorMinor := versionParts[0] + "." + versionParts[1]

	// Collect all matching versions and find the latest patch version
	var matchingVersions []MongoDBVersionInfo
	for _, v := range versions {
		// Skip RC and pre-release versions
		if strings.Contains(strings.ToLower(v.Version), "rc") ||
			strings.Contains(strings.ToLower(v.Version), "alpha") ||
			strings.Contains(strings.ToLower(v.Version), "beta") {
			continue
		}

		if strings.HasPrefix(v.Version, majorMinor+".") {
			matchingVersions = append(matchingVersions, v)
		}
	}

	if len(matchingVersions) == 0 {
		return "", fmt.Errorf("no versions found for MongoDB %s", majorMinor)
	}

	// Use semantic version comparison to find the latest version
	latestVersion := matchingVersions[0]
	latestVersionSemver := "v" + latestVersion.Version

	for _, v := range matchingVersions[1:] {
		currentSemver := "v" + v.Version
		// Compare using semver.Compare (returns -1 if current < latest, 0 if equal, 1 if current > latest)
		if semver.Compare(currentSemver, latestVersionSemver) > 0 {
			latestVersion = v
			latestVersionSemver = currentSemver
		}
	}

	return latestVersion.Version, nil
}

// extractArchive extracts a tar.gz archive to the target directory
func (m *Manager) extractArchive(archive io.Reader, targetDir string) error {
	// Create gzip reader
	gzReader, err := gzip.NewReader(archive)
	if err != nil {
		return fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Create tar reader
	tarReader := tar.NewReader(gzReader)

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar archive: %w", err)
		}

		// Skip the root directory (strip-components=1)
		parts := strings.Split(header.Name, "/")
		if len(parts) <= 1 {
			continue
		}
		relPath := filepath.Join(parts[1:]...)
		targetPath := filepath.Join(targetDir, relPath)

		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		case tar.TypeReg:
			// Create file
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return fmt.Errorf("failed to create file: %w", err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("failed to write file: %w", err)
			}
			outFile.Close()
		}
	}

	return nil
}

// extractZipArchive extracts a zip archive to the target directory
func (m *Manager) extractZipArchive(archive *os.File, targetDir string) error {
	// Get the size of the archive
	size, err := archive.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat archive: %w", err)
	}

	// Create zip reader
	zipReader, err := zip.NewReader(archive, size.Size())
	if err != nil {
		return fmt.Errorf("failed to create zip reader: %w", err)
	}

	// Extract files
	for _, file := range zipReader.File {
		// Skip the root directory (strip-components=1)
		parts := strings.Split(file.Name, "/")
		if len(parts) <= 1 {
			continue
		}
		relPath := filepath.Join(parts[1:]...)
		targetPath := filepath.Join(targetDir, relPath)

		if file.FileInfo().IsDir() {
			// Create directory
			if err := os.MkdirAll(targetPath, file.FileInfo().Mode()); err != nil {
				return fmt.Errorf("failed to create directory: %w", err)
			}
		} else {
			// Create file
			if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
				return fmt.Errorf("failed to create parent directory: %w", err)
			}

			sourceFile, err := file.Open()
			if err != nil {
				return fmt.Errorf("failed to open file in zip: %w", err)
			}

			outFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, file.FileInfo().Mode())
			if err != nil {
				sourceFile.Close()
				return fmt.Errorf("failed to create file: %w", err)
			}

			if _, err := io.Copy(outFile, sourceFile); err != nil {
				sourceFile.Close()
				outFile.Close()
				return fmt.Errorf("failed to write file: %w", err)
			}

			sourceFile.Close()
			outFile.Close()
		}
	}

	return nil
}

// ListInstalledVersions returns a list of installed MongoDB versions
// Checks PATH first, then permanent installation directory, then temp directories
func (m *Manager) ListInstalledVersions() ([]string, error) {
	versions := make([]string, 0)

	// Check if mongod is on PATH
	mongodPath, err := exec.LookPath("mongod")
	if err == nil {
		cmd := exec.Command(mongodPath, "--version")
		output, err := cmd.Output()
		if err == nil {
			versionRegex := regexp.MustCompile(`db version v(\d+\.\d+)`)
			matches := versionRegex.FindStringSubmatch(string(output))
			if len(matches) >= 2 {
				versions = append(versions, matches[1])
			}
		}
	}

	// Check permanent installation directory (~/.m/versions)
	if entries, err := os.ReadDir(m.installDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				version := entry.Name()
				// Verify it has a bin directory with mongod
				binDir := filepath.Join(m.installDir, version, "bin")
				mongodPath := filepath.Join(binDir, "mongod")
				if runtime.GOOS == "windows" {
					mongodPath = filepath.Join(binDir, "mongod.exe")
				}
				if _, err := os.Stat(mongodPath); err == nil {
					// Check if already in list
					found := false
					for _, v := range versions {
						if v == version {
							found = true
							break
						}
					}
					if !found {
						versions = append(versions, version)
					}
				}
			}
		}
	}

	// Check temp directories (for backwards compatibility)
	m.tempMu.Lock()
	defer m.tempMu.Unlock()

	for _, tempDir := range m.tempDirs {
		entries, err := os.ReadDir(tempDir)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "mongodb-") {
				// Extract version from directory name
				parts := strings.Split(entry.Name(), "-")
				if len(parts) >= 4 {
					version := parts[3]
					// Check if already in list
					found := false
					for _, v := range versions {
						if v == version {
							found = true
							break
						}
					}
					if !found {
						versions = append(versions, version)
					}
				}
			}
		}
	}

	return versions, nil
}

// IsVersionInstalled checks if a MongoDB version is installed
func (m *Manager) IsVersionInstalled(version string) (bool, error) {
	versions, err := m.ListInstalledVersions()
	if err != nil {
		return false, err
	}

	for _, v := range versions {
		if versionMatches(v, version) {
			return true, nil
		}
	}

	return false, nil
}

// GetVersionPath returns the path to the mongod binary for a specific version
func (m *Manager) GetVersionPath(version string) (string, error) {
	binPath, err := m.GetBinPath(version)
	if err != nil {
		return "", err
	}

	binaryName := "mongod"
	if runtime.GOOS == "windows" {
		binaryName = "mongod.exe"
	}

	mongodPath := filepath.Join(binPath, binaryName)

	// Verify the binary exists
	if _, err := os.Stat(mongodPath); err != nil {
		return "", fmt.Errorf("mongod binary not found at %s: %w", mongodPath, err)
	}

	return mongodPath, nil
}

// GetBinPath returns the bin directory path for a specific version
// First checks if mongod is on PATH with the correct version
// Then checks if version is installed in ~/.m/versions/{version}/bin
// If not, installs to permanent location and returns that path
func (m *Manager) GetBinPath(version string) (string, error) {
	// First, check if mongod is on PATH with the correct version
	binPath, found, err := m.checkMongoVersion(version)
	if err != nil {
		return "", fmt.Errorf("failed to check MongoDB version: %w", err)
	}

	if found {
		return binPath, nil
	}

	// Check if version is already installed in permanent location
	requiredSemver := normalizeVersion(version)
	requiredParts := strings.Split(strings.TrimPrefix(requiredSemver, "v"), ".")

	// If user specified patch version (X.Y.Z), check exact match
	if len(requiredParts) >= 3 {
		versionBinDir := filepath.Join(m.installDir, version, "bin")
		if _, err := os.Stat(versionBinDir); err == nil {
			// Verify mongod exists
			mongodPath := filepath.Join(versionBinDir, "mongod")
			if runtime.GOOS == "windows" {
				mongodPath = filepath.Join(versionBinDir, "mongod.exe")
			}
			if _, err := os.Stat(mongodPath); err == nil {
				return versionBinDir, nil
			}
		}
	} else {
		// If user specified minor version (X.Y), find latest matching patch version
		requiredMajorMinor := semver.MajorMinor(requiredSemver)

		// List all installed versions and find the latest matching patch
		if entries, err := os.ReadDir(m.installDir); err == nil {
			var matchingVersions []string
			for _, entry := range entries {
				if entry.IsDir() {
					installedVersion := entry.Name()
					installedSemver := normalizeVersion(installedVersion)

					// Check if major.minor matches using semver
					if semver.IsValid(installedSemver) {
						installedMajorMinor := semver.MajorMinor(installedSemver)
						if requiredMajorMinor == installedMajorMinor {
							// Verify it has a bin directory with mongod
							binDir := filepath.Join(m.installDir, installedVersion, "bin")
							mongodPath := filepath.Join(binDir, "mongod")
							if runtime.GOOS == "windows" {
								mongodPath = filepath.Join(binDir, "mongod.exe")
							}
							if _, err := os.Stat(mongodPath); err == nil {
								matchingVersions = append(matchingVersions, installedVersion)
							}
						}
					}
				}
			}

			// Find the latest version using semantic version comparison
			if len(matchingVersions) > 0 {
				latestVersion := matchingVersions[0]
				latestSemver := normalizeVersion(latestVersion)

				for _, v := range matchingVersions[1:] {
					currentSemver := normalizeVersion(v)
					if semver.Compare(currentSemver, latestSemver) > 0 {
						latestVersion = v
						latestSemver = currentSemver
					}
				}

				versionBinDir := filepath.Join(m.installDir, latestVersion, "bin")
				return versionBinDir, nil
			}
		}
	}

	// Not found, install to permanent location
	binPath, err = m.InstallVersion(version)
	if err != nil {
		return "", fmt.Errorf("failed to install MongoDB %s: %w", version, err)
	}

	return binPath, nil
}

// UseVersion verifies that the specified version is available
// This is useful if you want to use mongo shell or other utilities
func (m *Manager) UseVersion(version string) error {
	_, err := m.GetBinPath(version)
	if err != nil {
		return fmt.Errorf("MongoDB %s is not available: %w", version, err)
	}

	fmt.Printf("Using MongoDB %s\n", version)
	return nil
}

// Close cleans up temp directories
func (m *Manager) Close() error {
	m.tempMu.Lock()
	defer m.tempMu.Unlock()

	for _, tempDir := range m.tempDirs {
		if err := os.RemoveAll(tempDir); err != nil {
			// Log but don't fail
			fmt.Printf("Warning: failed to cleanup temp directory %s: %v\n", tempDir, err)
		}
	}

	m.tempDirs = nil
	return nil
}

// Uninstall removes all installed MongoDB versions and cleans up the installation directory
// It also removes any symlinks that might have been created
func (m *Manager) Uninstall() error {
	// Log which install directory is being used
	if m.installDir != "" {
		fmt.Printf("Using install directory: %s\n", m.installDir)
		// Remove the install directory (contains all versions)
		if _, err := os.Stat(m.installDir); err == nil {
			fmt.Printf("Removing install directory: %s\n", m.installDir)
			if err := os.RemoveAll(m.installDir); err != nil {
				return fmt.Errorf("failed to remove install directory %s: %w", m.installDir, err)
			}
		} else {
			fmt.Printf("Install directory does not exist: %s\n", m.installDir)
		}
	} else {
		return fmt.Errorf("install directory is not set")
	}

	// Try to remove parent .m directory if it exists and is empty
	// Only do this if installDir is the default ~/.m/versions AND M_INSTALL_DIR is not set
	if os.Getenv("M_INSTALL_DIR") == "" {
		homeDir, err := os.UserHomeDir()
		if err == nil {
			defaultInstallDir := filepath.Join(homeDir, ".m", "versions")
			if m.installDir == defaultInstallDir {
				mDir := filepath.Join(homeDir, ".m")
				if entries, err := os.ReadDir(mDir); err == nil && len(entries) == 0 {
					fmt.Printf("Removing empty .m directory: %s\n", mDir)
					if err := os.Remove(mDir); err != nil {
						// Log but don't fail - directory might not be empty or might not exist
						fmt.Printf("Warning: failed to remove .m directory %s: %v\n", mDir, err)
					}
				}
			}
		}
	}

	// Look for and remove any symlinks that might have been created
	// Common locations for symlinks: ~/.local/bin, /usr/local/bin, ~/bin
	if err := m.removeSymlinks(); err != nil {
		// Log but don't fail - symlinks might not exist
		fmt.Printf("Warning: failed to remove some symlinks: %v\n", err)
	}

	return nil
}

// removeSymlinks searches for and removes symlinks pointing to MongoDB binaries
func (m *Manager) removeSymlinks() error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil // Can't find home directory, skip symlink removal
	}

	// Common locations where symlinks might be created
	searchPaths := []string{
		filepath.Join(homeDir, ".local", "bin"),
		filepath.Join(homeDir, "bin"),
		"/usr/local/bin",
	}

	// MongoDB binary names to look for
	mongoBinaries := []string{"mongod", "mongos", "mongo", "mongosh", "mongoimport", "mongoexport", "mongorestore", "mongodump"}

	var removed []string
	for _, searchPath := range searchPaths {
		// Skip if directory doesn't exist
		if _, err := os.Stat(searchPath); os.IsNotExist(err) {
			continue
		}

		entries, err := os.ReadDir(searchPath)
		if err != nil {
			continue
		}

		for _, entry := range entries {
			entryName := entry.Name()
			// Check if this is a MongoDB binary name
			isMongoBinary := false
			for _, binName := range mongoBinaries {
				if entryName == binName || entryName == binName+".exe" {
					isMongoBinary = true
					break
				}
			}

			if !isMongoBinary {
				continue
			}

			entryPath := filepath.Join(searchPath, entryName)
			info, err := os.Lstat(entryPath)
			if err != nil {
				continue
			}

			// Check if it's a symlink
			if info.Mode()&os.ModeSymlink != 0 {
				// Check if symlink points to our install directory
				linkTarget, err := os.Readlink(entryPath)
				if err != nil {
					continue
				}

				// Resolve absolute path of link target
				// If linkTarget is relative, resolve it relative to the directory containing the symlink
				var absLinkTarget string
				if filepath.IsAbs(linkTarget) {
					absLinkTarget = linkTarget
				} else {
					absLinkTarget = filepath.Join(searchPath, linkTarget)
					absLinkTarget, err = filepath.Abs(absLinkTarget)
					if err != nil {
						continue
					}
				}

				// Check if the symlink points to our install directory
				if strings.HasPrefix(absLinkTarget, m.installDir) {
					fmt.Printf("Removing symlink: %s -> %s\n", entryPath, linkTarget)
					if err := os.Remove(entryPath); err != nil {
						fmt.Printf("Warning: failed to remove symlink %s: %v\n", entryPath, err)
					} else {
						removed = append(removed, entryPath)
					}
				}
			}
		}
	}

	if len(removed) > 0 {
		fmt.Printf("Removed %d symlink(s)\n", len(removed))
	}

	return nil
}
