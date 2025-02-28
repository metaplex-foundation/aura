#!/bin/bash
set -e

# This script updates version numbers in all Cargo.toml files and other version files
# Usage: update_versions.sh <version>

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Error: Version is required"
  exit 1
fi

# Update Cargo.toml versions
find . -name "Cargo.toml" -type f -exec cargo set-version "$VERSION" --manifest-path {} \;

# Update any other version references (add any other files that contain version numbers)
if [ -f "VERSION" ]; then
  echo "$VERSION" > VERSION
fi

echo "Updated version numbers to $VERSION"

# Commit the changes
git add -A
git commit -m "chore: bump version to $VERSION"

echo "Version changes committed"