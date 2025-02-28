#!/bin/bash
set -e

# This script extracts the relevant section from the changelog file for the PR description
# Usage: extract_changelog_section.sh <output_file>

OUTPUT_FILE=$1

if [ -z "$OUTPUT_FILE" ]; then
  echo "Error: Output file is required"
  exit 1
fi

if [ ! -f "CHANGELOG.md" ]; then
  echo "Error: CHANGELOG.md does not exist"
  exit 1
fi

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