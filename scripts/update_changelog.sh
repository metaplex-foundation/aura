#!/bin/bash
set -e

# This script updates the CHANGELOG.md by appending new changes instead of regenerating it
# It takes two parameters:
#   $1: New version tag (e.g., v0.5.0)
#   $2: Previous version tag (optional, will be detected automatically if not provided)

NEW_VERSION=$1
PREVIOUS_VERSION=$2

if [ -z "$NEW_VERSION" ]; then
  echo "Error: New version tag is required"
  echo "Usage: $0 <new_version_tag> [previous_version_tag]"
  exit 1
fi

# If previous version is not provided, find the latest tag
if [ -z "$PREVIOUS_VERSION" ]; then
  PREVIOUS_VERSION=$(git describe --tags --abbrev=0 2>/dev/null || echo "")
  
  if [ -z "$PREVIOUS_VERSION" ]; then
    # If no tags exist, use the first commit
    PREVIOUS_VERSION=$(git rev-list --max-parents=0 HEAD)
    echo "No previous tags found, using first commit: $PREVIOUS_VERSION"
  else
    echo "Using latest tag as previous version: $PREVIOUS_VERSION"
  fi
fi

# Define filenames
TEMP_CHANGELOG="CHANGELOG.temp.md"
FINAL_CHANGELOG="CHANGELOG.md"
HEADER_FILE="CHANGELOG.header.md"
NEW_CONTENT_FILE="CHANGELOG.new.md"

# Check if existing changelog exists
if [ ! -f "$FINAL_CHANGELOG" ]; then
  echo "No existing CHANGELOG.md found, will create a new one"
  touch "$FINAL_CHANGELOG"
fi

# Check if the new version tag exists, use HEAD if it doesn't
if ! git rev-parse "$NEW_VERSION" >/dev/null 2>&1; then
  echo "Tag $NEW_VERSION doesn't exist yet, using HEAD for changelog generation"
  RANGE_END="HEAD"
else
  RANGE_END="$NEW_VERSION"
fi

# Create a temporary changelog for just the new changes
if [ "$RANGE_END" = "HEAD" ]; then
  echo "Generating changelog for $PREVIOUS_VERSION..HEAD with tag $NEW_VERSION"
  git-cliff --config cliff.toml --tag "$NEW_VERSION" --output "$TEMP_CHANGELOG" "$PREVIOUS_VERSION..HEAD"
else
  echo "Generating changelog for $PREVIOUS_VERSION..$NEW_VERSION"
  git-cliff --config cliff.toml --tag "$NEW_VERSION" --output "$TEMP_CHANGELOG" "$PREVIOUS_VERSION..$NEW_VERSION"
fi

# Extract the header (everything before the first version section)
awk 'BEGIN{header=1} /^## \[.*\]/{if(header) {header=0}} header{print}' "$TEMP_CHANGELOG" > "$HEADER_FILE"

# Extract the new content (just the first version section)
awk 'BEGIN{section=0; found=0} 
  /^## \[.*\]/{
    if(!found) {
      section=1; 
      found=1;
      print;
    } else {
      section=0;
    }
    next;
  } 
  section{print}' "$TEMP_CHANGELOG" > "$NEW_CONTENT_FILE"

# Check if we got any new content
if [ ! -s "$NEW_CONTENT_FILE" ]; then
  echo "No new content generated. Check your version tags and commit history."
  rm -f "$TEMP_CHANGELOG" "$HEADER_FILE" "$NEW_CONTENT_FILE"
  exit 1
fi

# Create the final changelog by combining header, new content, and existing content (minus the header)
{
  cat "$HEADER_FILE"
  cat "$NEW_CONTENT_FILE"
  if [ -s "$FINAL_CHANGELOG" ]; then
    # Skip the header from the existing changelog
    awk 'BEGIN{header=1} /^## \[.*\]/{if(header) {header=0; print; next}} !header{print}' "$FINAL_CHANGELOG"
  fi
} > "$TEMP_CHANGELOG"

# Move the temporary changelog to the final location
mv "$TEMP_CHANGELOG" "$FINAL_CHANGELOG"

# Clean up temporary files
rm -f "$HEADER_FILE" "$NEW_CONTENT_FILE"

echo "Updated CHANGELOG.md with changes from $PREVIOUS_VERSION to $NEW_VERSION"