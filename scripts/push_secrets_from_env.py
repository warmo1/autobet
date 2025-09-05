"""
Push selected secrets from a local .env into Google Secret Manager and
optionally update a Cloud Run service to reference them.

Usage examples:

  # Push standard secrets from autobet/.env into project autobet-470818
  python scripts/push_secrets_from_env.py \
      --project autobet-470818 \
      --service-account run-ingest-sa@autobet-470818.iam.gserviceaccount.com

  # Also wire the Cloud Run service to use these secrets (London region)
  python scripts/push_secrets_from_env.py \
      --project autobet-470818 \
      --service-account run-ingest-sa@autobet-470818.iam.gserviceaccount.com \
      --update-service autobet-ingest --region europe-west2

Notes:
  - This script shells out to `gcloud`; ensure you are logged in and that the
    active project matches `--project` (or pass --project explicitly).
  - It never prints secret values. Make sure your .env is not committed.
"""
from __future__ import annotations

import argparse
import os
import shlex
import subprocess
from pathlib import Path
from typing import Dict, List

from dotenv import dotenv_values


DEFAULT_SECRET_VARS = [
    "TELEGRAM_TOKEN",
    "GEMINI_API_KEY",
    "OPENAI_API_KEY",
    "OWM_API_KEY",
    "TOTE_API_KEY",
]


def run(cmd: List[str], *, input_bytes: bytes | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, input=input_bytes)


def gcloud_project() -> str:
    try:
        out = subprocess.check_output(["gcloud", "config", "get-value", "project"], text=True)
        return out.strip()
    except Exception:
        return ""


def ensure_secret(project: str, name: str) -> None:
    # If secret exists, do nothing; else create with automatic replication
    try:
        subprocess.run(["gcloud", "secrets", "describe", name, "--project", project], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError:
        run(["gcloud", "secrets", "create", name, "--replication-policy=automatic", "--project", project])


def add_secret_version(project: str, name: str, value: str) -> None:
    if value is None or value == "":
        raise ValueError(f"Secret {name} has empty value in .env")
    run(["gcloud", "secrets", "versions", "add", name, "--project", project, "--data-file=-"], input_bytes=value.encode("utf-8"))


def grant_accessor(project: str, name: str, service_account: str) -> None:
    run([
        "gcloud", "secrets", "add-iam-policy-binding", name,
        "--project", project,
        "--member", f"serviceAccount:{service_account}",
        "--role", "roles/secretmanager.secretAccessor",
    ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Push secrets from .env to Secret Manager and optionally update Cloud Run")
    parser.add_argument("--project", required=False, help="GCP project ID (defaults to current gcloud project)")
    parser.add_argument("--env", default=str(Path(__file__).resolve().parents[1] / ".env"), help="Path to .env file (default: autobet/.env)")
    parser.add_argument("--service-account", required=False, help="Service account to grant secret access to")
    parser.add_argument("--secrets", nargs="*", default=DEFAULT_SECRET_VARS, help="Env var names to push as secrets")
    parser.add_argument("--update-service", dest="service", help="Cloud Run service name to update (optional)")
    parser.add_argument("--region", help="Cloud Run region (required if --update-service is used)")

    args = parser.parse_args()
    project = args.project or gcloud_project()
    if not project:
        raise SystemExit("Project not set. Pass --project or run 'gcloud config set project ...'.")

    env_path = Path(args.env)
    if not env_path.exists():
        raise SystemExit(f".env not found at {env_path}")
    env: Dict[str, str] = {k: v for k, v in dotenv_values(env_path).items() if k}

    pushed: List[str] = []
    for name in args.secrets:
        val = (env.get(name) or "").strip()
        if not val:
            # Skip empty; warn but continue
            print(f"[skip] {name}: empty or not set in .env")
            continue
        print(f"[push] {name}: ensuring secret and adding version…")
        ensure_secret(project, name)
        add_secret_version(project, name, val)
        if args.service_account:
            grant_accessor(project, name, args.service_account)
        pushed.append(name)

    if args.service:
        if not args.region:
            raise SystemExit("--region is required when using --update-service")
        if not pushed:
            print("No secrets pushed; skipping Cloud Run update.")
            return
        # Build --set-secrets flag value like NAME=NAME:latest for each pushed secret
        mapping = ",".join(f"{n}={n}:latest" for n in pushed)
        print(f"[update] Cloud Run service {args.service} in {args.region}: setting secrets for {', '.join(pushed)}")
        run([
            "gcloud", "run", "services", "update", args.service,
            "--platform=managed",
            "--region", args.region,
            "--set-secrets", mapping,
        ])

    print("Done.")


if __name__ == "__main__":
    main()

