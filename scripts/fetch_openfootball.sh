#!/usr/bin/env bash
set -euo pipefail

# --- Find the Project's Root Directory ---
# This makes the script runnable from anywhere.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# **FIX**: The project root is one level up from the scripts directory.
PROJECT_ROOT="$( dirname "${SCRIPT_DIR}" )"

# --- Define Paths Relative to the Project Root ---
TARGET_DIR="${PROJECT_ROOT}/data/football/openfootball"
REPO_URL="https://github.com/openfootball/england.git"

# --- Script Logic ---
echo "Automating download of openfootball data..."

# Check if the directory already exists
if [ -d "${TARGET_DIR}" ]; then
  echo "Directory already exists. Pulling latest changes..."
  (cd "${TARGET_DIR}" && git pull)
else
  echo "Cloning repository for the first time..."
  git clone "${REPO_URL}" "${TARGET_DIR}"
fi

echo "Successfully downloaded/updated openfootball data into ${TARGET_DIR}"
