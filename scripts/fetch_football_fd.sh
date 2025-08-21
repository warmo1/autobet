#!/usr/bin/env bash
set -euo pipefail
mkdir -p data/football/E0 data/football/E1 data/football/fixtures
base="https://www.football-data.co.uk/mmz4281"
# Adjust seasons as needed (2425=2024/25, etc.)
for yy in 2324 2425; do
  wget -N "${base}/${yy}/E0.csv" -P data/football/E0/
  wget -N "${base}/${yy}/E1.csv" -P data/football/E1/
done
wget -N "https://www.football-data.co.uk/fixtures.csv" -P data/football/fixtures/
echo "Downloaded football CSVs into data/football"
