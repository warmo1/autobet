#!/usr/bin/env bash
set -euo pipefail

# --- Configuration ---
# Add any new repository names from the openfootball GitHub org to this list
REPOS_TO_DOWNLOAD=("england" "de-deutschland" "es-spain" "it-italy")

# --- Argument Check ---
if [ -z "$1" ] || ( [ "$1" != "init" ] && [ "$1" != "update" ] ); then
  echo "Error: Please provide a mode: 'init' for initial bulk download, or 'update' for daily updates."
  exit 1
fi
MODE=$1

# --- Find the Project's Root Directory ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( dirname "${SCRIPT_DIR}" )"
BASE_TARGET_DIR="${PROJECT_ROOT}/data/football/openfootball"

# --- Script Logic ---
if [ "$MODE" == "init" ]; then
  echo "--- Running in INITIAL LOAD mode ---"
  for repo in "${REPOS_TO_DOWNLOAD[@]}"; do
    TARGET_DIR="${BASE_TARGET_DIR}/${repo}"
    REPO_URL="https://github.com/openfootball/${repo}.git"
    if [ -d "${TARGET_DIR}" ]; then
      echo "Repository '${repo}' already exists. Skipping clone."
    else
      echo "Cloning new repository '${repo}'..."
      mkdir -p "${TARGET_DIR}"
      git clone "${REPO_URL}" "${TARGET_DIR}"
    fi
  done
  echo "Initial load complete."

elif [ "$MODE" == "update" ]; then
  echo "--- Running in UPDATE mode ---"
  if [ ! -d "${BASE_TARGET_DIR}" ]; then
    echo "No data found to update. Run 'init' mode first."
    exit 1
  fi
  # Find all git repositories inside the openfootball data folder and pull changes
  find "${BASE_TARGET_DIR}" -mindepth 1 -maxdepth 1 -type d -exec echo "Updating repository:" {} \; -exec git -C {} pull \;
  echo "Update complete."
fi
