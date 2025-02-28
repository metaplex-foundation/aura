#!/bin/bash
set -e

# This script sets up the environment for release preparation

echo "Setting up Git identity..."
git config --global user.name "GitHub Actions"
git config --global user.email "actions@github.com"

echo "Installing dependencies..."
sudo apt-get update && sudo apt-get install -y protobuf-compiler
cargo install cargo-edit
cargo install git-cliff

echo "Environment setup complete"