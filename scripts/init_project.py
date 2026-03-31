#!/usr/bin/env python3
"""
scripts/init_project.py

One-command project bootstrapper. Run this after cloning to go from zero
to a fully validated, locally-runnable state.

What it does:
  1. Checks system prerequisites (Python version, Docker, disk space)
  2. Generates secure .env from .env.example (auto-fills all secrets)
  3. Creates required local data directories
  4. Runs the standalone validator
  5. Prints the next steps

Usage:
    python scripts/init_project.py
    python scripts/init_project.py --skip-docker-check
    python scripts/init_project.py --no-generate-env  # use existing .env
"""
from __future__ import annotations

import argparse
import os
import platform
import secrets
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent

GREEN  = "\033[32m"
YELLOW = "\033[33m"
RED    = "\033[31m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def ok(msg: str)   -> None: print(f"  {GREEN}✓{RESET}  {msg}")
def warn(msg: str) -> None: print(f"  {YELLOW}⚠{RESET}  {msg}")
def err(msg: str)  -> None: print(f"  {RED}✗{RESET}  {msg}")
def info(msg: str) -> None: print(f"     {msg}")


# ── Prerequisite checks ───────────────────────────────────────────────────────

def check_python() -> bool:
    major, minor = sys.version_info[:2]
    if major == 3 and minor >= 11:
        ok(f"Python {major}.{minor} ✓")
        return True
    else:
        err(f"Python {major}.{minor} found — Python 3.11+ required")
        info("Install via: https://www.python.org/downloads/")
        return False


def check_docker(skip: bool = False) -> bool:
    if skip:
        warn("Docker check skipped (--skip-docker-check)")
        return True
    docker = shutil.which("docker")
    if not docker:
        warn("Docker not found — required for the full stack")
        info("Install: https://docs.docker.com/get-docker/")
        return False
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=10
        )
        if result.returncode == 0:
            ok("Docker is running ✓")
            return True
        else:
            warn("Docker is installed but not running — start Docker Desktop")
            return False
    except (FileNotFoundError, subprocess.TimeoutExpired):
        warn("Docker check failed — is Docker running?")
        return False


def check_disk_space(min_gb: float = 5.0) -> bool:
    try:
        usage = shutil.disk_usage(ROOT)
        free_gb = usage.free / (1024 ** 3)
        if free_gb >= min_gb:
            ok(f"{free_gb:.1f} GB free disk space ✓")
            return True
        else:
            warn(f"Only {free_gb:.1f} GB free — recommend at least {min_gb} GB")
            return False
    except Exception:
        warn("Could not check disk space")
        return False


# ── Secret generation ─────────────────────────────────────────────────────────

def generate_fernet_key() -> str:
    """Generate an Airflow Fernet key."""
    try:
        from cryptography.fernet import Fernet
        return Fernet.generate_key().decode()
    except ImportError:
        # cryptography not installed yet — generate a placeholder
        import base64
        key_bytes = secrets.token_bytes(32)
        return base64.urlsafe_b64encode(key_bytes).decode()


def generate_env(force: bool = False) -> bool:
    env_file     = ROOT / ".env"
    example_file = ROOT / ".env.example"

    if not example_file.exists():
        err(".env.example not found — repository may be incomplete")
        return False

    if env_file.exists() and not force:
        warn(".env already exists — skipping generation (use --force to overwrite)")
        return True

    example_content = example_file.read_text()

    # Generate secure values for all secret placeholders
    replacements = {
        "change_me_generate_with_fernet_generate_key": generate_fernet_key(),
        "change_me_random_32_chars":                   secrets.token_urlsafe(32),
        "change_me_generate_with_secrets_token_hex":   secrets.token_hex(32),
    }

    env_content = example_content
    for placeholder, value in replacements.items():
        env_content = env_content.replace(placeholder, value)

    # Use a unique value for each occurrence of the generic placeholder
    while "change_me_random_32_chars" in env_content:
        env_content = env_content.replace(
            "change_me_random_32_chars",
            secrets.token_urlsafe(32),
            1,  # replace one at a time
        )

    env_file.write_text(env_content)
    ok(f".env generated with secure random secrets at {env_file}")
    return True


# ── Directory setup ───────────────────────────────────────────────────────────

def create_data_dirs() -> None:
    dirs = [
        ROOT / "data" / "bronze" / "orders",
        ROOT / "data" / "silver" / "orders",
        ROOT / "data" / "gold",
        ROOT / "data" / "gold" / "audit",
        ROOT / "data" / "bronze" / "events",
        ROOT / "data" / "silver" / "sessions",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    ok(f"Data directories created under {ROOT / 'data'}/")


# ── Run validator ─────────────────────────────────────────────────────────────

def run_validator() -> bool:
    validator = ROOT / "scripts" / "validate.py"
    if not validator.exists():
        warn("Validator script not found — skipping")
        return True

    result = subprocess.run(
        [sys.executable, str(validator)],
        cwd=ROOT,
    )
    return result.returncode == 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="RetailPulse project initialiser")
    parser.add_argument("--skip-docker-check", action="store_true")
    parser.add_argument("--no-generate-env",   action="store_true")
    parser.add_argument("--force-env",         action="store_true", help="Overwrite existing .env")
    args = parser.parse_args()

    print(f"\n{BOLD}RetailPulse — Project Initialisation{RESET}")
    print("=" * 48)

    print(f"\n{BOLD}1. System prerequisites{RESET}")
    python_ok = check_python()
    docker_ok = check_docker(skip=args.skip_docker_check)
    disk_ok   = check_disk_space()

    if not python_ok:
        print(f"\n{RED}Python 3.11+ is required. Exiting.{RESET}")
        sys.exit(1)

    print(f"\n{BOLD}2. Environment configuration{RESET}")
    if not args.no_generate_env:
        env_ok = generate_env(force=args.force_env)
    else:
        env_ok = (ROOT / ".env").exists()
        if env_ok:
            ok(".env exists (--no-generate-env)")
        else:
            err(".env not found — run without --no-generate-env")

    print(f"\n{BOLD}3. Data directories{RESET}")
    create_data_dirs()

    print(f"\n{BOLD}4. Project validation{RESET}")
    validator_ok = run_validator()

    print(f"\n{'=' * 48}")
    print(f"{BOLD}Summary{RESET}")
    all_ok = python_ok and env_ok and validator_ok
    if all_ok:
        print(f"  {GREEN}{BOLD}✓ Project is ready!{RESET}\n")
        print("  Next steps:")
        print(f"    {BOLD}make setup{RESET}         — create Python venv + install deps")
        print(f"    {BOLD}make docker-up{RESET}     — start Kafka, Airflow, Postgres, API")
        print(f"    {BOLD}make dbt-run{RESET}       — build Silver→Gold models")
        print(f"    {BOLD}make test-unit{RESET}     — run all unit tests")
        print(f"    {BOLD}make serve{RESET}         — start FastAPI at http://localhost:8000/docs")
        print()
    else:
        print(f"  {YELLOW}⚠ Some checks need attention — see above.{RESET}")
        if not docker_ok:
            print("  → Start Docker Desktop and re-run to use the full stack")
        if not validator_ok:
            print("  → Run python scripts/validate.py for details")
        sys.exit(1 if not (python_ok and env_ok) else 0)


if __name__ == "__main__":
    main()
