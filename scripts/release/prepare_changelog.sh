#!/bin/bash
set -e

# This script generates a changelog for a new version
# Usage: prepare_changelog.sh <version>

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Error: Version is required"
  echo "Usage: $0 <version>"
  exit 1
fi

# Find the previous version tag
PREVIOUS_VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
if [ -z "$PREVIOUS_VERSION" ]; then
  echo "No previous tags found. Will generate full changelog."
  PREVIOUS_VERSION=""
else
  echo "Found previous version tag: $PREVIOUS_VERSION"
  # Export for GitHub Actions if needed
  if [ -n "$GITHUB_OUTPUT" ]; then
    echo "previous_version=$PREVIOUS_VERSION" >> "$GITHUB_OUTPUT"
  fi
fi

# Generate changelog incrementally or in full
if [ -n "$PREVIOUS_VERSION" ]; then
  echo "Generating incremental changelog from $PREVIOUS_VERSION to v$VERSION"
  ./scripts/update_changelog.sh "v$VERSION" "$PREVIOUS_VERSION"
else
  echo "Generating full changelog for v$VERSION"
  git-cliff --config cliff.toml --tag "v$VERSION" --output CHANGELOG.md
fi

# Verify the changelog
./scripts/release/verify_changelog.sh "$VERSION"
  
# Extract changelog section for PR description
./scripts/release/extract_changelog_section.sh .changelog_content

# Commit changes
git add CHANGELOG.md
git commit -m "docs: add changelog for v$VERSION"

echo "Changelog for v$VERSION has been generated and committed"