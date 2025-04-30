#!/bin/bash

# This script adds comment with the release number in which this Task is included in the LINEAR

LINEAR_API_KEY=$1
GITHUB_RELEASE_TAG=$2

if [ -z "$LINEAR_API_KEY" ]; then
  echo "Error: Linear API KEY is required"
  exit 1
fi

if [ -z "$GITHUB_RELEASE_TAG" ]; then
  echo "Error: Git Hub release tag is required"
  exit 1
fi

# Check if the .release_tasks file exists
if [ ! -f .release_tasks ]; then
    echo "Error: .release_tasks file not found"
    exit 1
fi

# Check if jq is installed (required for parsing JSON responses)
if ! command -v jq &> /dev/null; then
    echo "jq not found. Please install it using 'sudo apt install jq' (Linux) or 'brew install jq' (macOS)."
    exit 1
fi

# Read ticket IDs from the file and update each task
while read -r ISSUE_ID; do
    echo "Updating Linear issue $ISSUE_ID"

    # Add a comment to the task
    RESPONSE=$(curl -s -X POST "https://api.linear.app/graphql" \
        -H "Authorization: $LINEAR_API_KEY" \
        -H "Content-Type: application/json" \
        --data '{
          "query": "mutation { commentCreate(input: { issueId: \"'"$ISSUE_ID"'\", body: \"The task is part of the release v'$GITHUB_RELEASE_TAG' \" }) { success } }"
        }')

    SUCCESS=$(echo "$RESPONSE" | jq -r '.data.commentCreate.success')
    if [ "$SUCCESS" != "true" ]; then
        echo "❌ Failed to add comment for $ISSUE_ID: $RESPONSE"
    else
        echo "✅ Successfully updated $ISSUE_ID"
    fi

done < .release_tasks
