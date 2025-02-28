#!/bin/bash
set -e

# This script verifies that the changelog file exists and contains the expected content
# Usage: verify_changelog.sh <version>

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Error: Version is required"
  exit 1
fi

# Check that the changelog file exists and has content
if [ ! -s "CHANGELOG.md" ]; then
  echo "Error: CHANGELOG.md is empty or does not exist"
  exit 1
fi

# Check that the changelog contains the version we're releasing
if ! grep -q "v$VERSION" CHANGELOG.md; then
  echo "Error: CHANGELOG.md does not contain version v$VERSION"
  echo "Contents of CHANGELOG.md:"
  cat CHANGELOG.md
  exit 1
fi

# Check that the changelog has sections
if ! grep -q "###" CHANGELOG.md; then
  echo "Warning: CHANGELOG.md does not contain any sections (###)"
  echo "This might be ok if there are no conventional commits, but please verify"
fi

echo "Changelog verification passed!"