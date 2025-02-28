#!/bin/bash
set -e

# This script validates a version and creates a release branch
# Usage: create_release_branch.sh <version>

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Error: Version is required"
  echo "Usage: $0 <version>"
  exit 1
fi

# Validate version format
./scripts/release/validate_version.sh "$VERSION"

# Create release branch
RELEASE_BRANCH="release/v$VERSION"
echo "Creating branch $RELEASE_BRANCH"
git checkout -b "$RELEASE_BRANCH"

# Export the branch name for GitHub Actions
if [ -n "$GITHUB_ENV" ]; then
  echo "RELEASE_BRANCH=$RELEASE_BRANCH" >> "$GITHUB_ENV"
  echo "Branch name exported to GITHUB_ENV: $RELEASE_BRANCH"
fi

echo "Release branch $RELEASE_BRANCH created successfully"