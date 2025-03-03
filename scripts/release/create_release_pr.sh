#!/bin/bash
set -e

# This script pushes a release branch and creates a PR
# Usage: create_release_pr.sh <version_tag> <target_branch> <source_branch>

VERSION_TAG=$1
TARGET_BRANCH=$2
SOURCE_BRANCH=$3

if [ -z "$VERSION_TAG" ] || [ -z "$TARGET_BRANCH" ] || [ -z "$SOURCE_BRANCH" ]; then
  echo "Error: Required parameters missing"
  echo "Usage: $0 <version_tag> <target_branch> <source_branch>"
  exit 1
fi

# Remove v prefix if present
VERSION=${VERSION_TAG#v}

echo "Pushing release branch $SOURCE_BRANCH..."
git push -u origin "$SOURCE_BRANCH"

# Create the PR using GitHub CLI
echo "Creating pull request from $SOURCE_BRANCH to $TARGET_BRANCH for $VERSION_TAG..."
PR_URL=$(gh pr create \
  --base "$TARGET_BRANCH" \
  --head "$SOURCE_BRANCH" \
  --title "Release $VERSION_TAG" \
  --body-file .changelog_content \
  --draft false)

echo "Pull Request created: $PR_URL"
echo "Please review the PR, make any necessary adjustments, and merge when ready."
