#!/usr/bin/env bash
set -euo pipefail

# Define file paths
DATA_DIR="data/cricket"
ZIP_FILE="${DATA_DIR}/cricsheet_all_csv.zip"
CSV_DIR="${DATA_DIR}/csv"

# Ensure directories exist
mkdir -p "${DATA_DIR}"
mkdir -p "${CSV_DIR}"

# Download the zip file
echo "Downloading Cricsheet data..."
wget -N https://cricsheet.org/downloads/all_csv.zip -O "${ZIP_FILE}"

# Unzip the contents, overwriting any existing files
echo "Unzipping files to ${CSV_DIR}..."
unzip -o "${ZIP_FILE}" -d "${CSV_DIR}"

# **FIX**: Clean up the zip file after successful extraction
echo "Cleaning up zip file..."
rm "${ZIP_FILE}"

echo "Successfully downloaded and extracted Cricsheet CSVs into ${CSV_DIR}"
