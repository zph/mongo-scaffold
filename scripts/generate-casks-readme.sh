#!/bin/bash
# Generate README.md in the Casks folder for Homebrew installation instructions

mkdir -p dist/homebrew/Casks

cat > dist/homebrew/Casks/README.md << 'EOF'
# mongo-scaffold Homebrew Cask

## Installation

To install mongo-scaffold via Homebrew:

```bash
brew tap zph/mongo-scaffold https://github.com/zph/mongo-scaffold
brew install zph/mongo-scaffold/mongo-scaffold
```

This will install the following binaries:
- `m` - MongoDB version manager
- `mlaunch` - MongoDB cluster launcher
- `mongo-cluster` - MongoDB cluster management tool

## Usage

After installation, you can use the binaries directly:

```bash
m --version
mlaunch --help
mongo-cluster --help
```

## Updating

To update to the latest version:

```bash
brew upgrade mongo-scaffold
```

## Uninstalling

To uninstall:

```bash
brew uninstall mongo-scaffold
brew untap zph/mongo-scaffold
```
EOF

echo "Generated README.md in dist/homebrew/Casks/"

