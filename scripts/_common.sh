#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root (two levels up from this script)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load .env from autobet/.env if present
if [[ -f "$REPO_ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$REPO_ROOT/.env"
  set +a
fi

# Prefer python3, fallback to python
PY=python3
command -v python >/dev/null 2>&1 && PY=python

# Ensure sports package resolvable for Python scripts
# Append to PYTHONPATH safely even if it's currently unset
if [[ -z "${PYTHONPATH+x}" ]]; then
  export PYTHONPATH="$REPO_ROOT"
else
  export PYTHONPATH="$REPO_ROOT:$PYTHONPATH"
fi
