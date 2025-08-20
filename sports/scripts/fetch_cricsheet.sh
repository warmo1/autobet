#!/usr/bin/env bash
set -euo pipefail
mkdir -p data/cricket
wget -N https://cricsheet.org/downloads/all_csv.zip -O data/cricket/cricsheet_all_csv.zip
unzip -o data/cricket/cricsheet_all_csv.zip -d data/cricket/csv/
echo "Downloaded Cricsheet CSVs into data/cricket/csv"
