#!/usr/bin/env bash
set -euo pipefail

# --- Find the Project's Root Directory ---
# This makes the script runnable from anywhere.
# It finds the script's own location, then goes up two levels (from /autobet/sports/scripts to /autobet).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$( dirname "$( dirname "${SCRIPT_DIR}" )" )"

# --- Define Paths Relative to the Project Root ---
DATA_DIR="${PROJECT_ROOT}/data/cricket"
ZIP_FILE="${DATA_DIR}/cricsheet_all_csv.zip"
CSV_DIR="${DATA_DIR}/csv"

# --- Script Logic (remains the same) ---
# Ensure directories exist
mkdir -p "${DATA_DIR}"
mkdir -p "${CSV_DIR}"

# Download the zip file
echo "Downloading Cricsheet data to ${ZIP_FILE}..."
wget -N https://cricsheet.org/downloads/all_csv.zip -O "${ZIP_FILE}"

# Unzip the contents, overwriting any existing files
echo "Unzipping files to ${CSV_DIR}..."
unzip -o "${ZIP_FILE}" -d "${CSV_DIR}"

# Clean up the zip file after successful extraction
echo "Cleaning up zip file..."
rm "${ZIP_FILE}"

echo "Successfully downloaded and extracted Cricsheet CSVs into ${CSV_DIR}"
