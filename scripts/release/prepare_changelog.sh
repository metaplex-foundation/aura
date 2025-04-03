#!/bin/bash
set -e

# This script generates a changelog for a new version, verifies it, and extracts the section for PR
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
echo "Verifying changelog..."
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
  
# Extract changelog section for PR description
echo "Extracting changelog section for PR description..."
OUTPUT_FILE=".changelog_content"

# Generate file with list of tasks included this release
grep -o 'MTG-[0-9]\{4\}' .changelog_content | sort -u > ".release_tasks"

# Extract just the section for this version
awk 'BEGIN{section=0; found=0} 
  /^## \[.*\]/{
    if(!found) {
      section=1; 
      found=1;
    } else {
      section=0;
    }
  } 
  section{print}' CHANGELOG.md | grep -v "^\[" > "$OUTPUT_FILE"

# Check if we extracted any content
if [ ! -s "$OUTPUT_FILE" ]; then
  echo "No content extracted from CHANGELOG.md. Check your changelog file."
  echo "No changelog content found" > "$OUTPUT_FILE"
  exit 1
fi

echo "Extracted changelog section to $OUTPUT_FILE"

# Commit changes
git add CHANGELOG.md
git commit -m "docs: add changelog for v$VERSION"

echo "Changelog for v$VERSION has been generated and committed"
