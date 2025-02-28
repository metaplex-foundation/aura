#!/bin/bash
set -e

# This script validates that a version number follows semantic versioning format
# Usage: validate_version.sh <version>

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Error: Version is required"
  exit 1
fi

# Make sure the version follows semantic versioning format
if ! echo "$VERSION" | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "Error: Version must follow semantic versioning format (e.g., 0.5.0)"
  exit 1
fi

echo "Version $VERSION is valid"