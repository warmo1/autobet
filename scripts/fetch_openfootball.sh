#!/usr/bin/env bash
set -euo pipefail

# --- Find the Project's Root Directory ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( dirname "$( dirname "${SCRIPT_DIR}" )" )"

# --- Define Paths ---
TARGET_DIR="${PROJECT_ROOT}/data/football/openfootball"
REPO_URL="https://github.com/openfootball/england.git"

# --- Script Logic ---
echo "Automating download of openfootball data..."

# Check if the directory already exists
if [ -d "${TARGET_DIR}" ]; then
  echo "Directory already exists. Pulling latest changes..."
  # If it exists, pull the latest changes
  (cd "${TARGET_DIR}" && git pull)
else
  echo "Cloning repository for the first time..."
  # If it doesn't exist, clone the repository
  git clone "${REPO_URL}" "${TARGET_DIR}"
fi

echo "Successfully downloaded/updated openfootball data into ${TARGET_DIR}"
