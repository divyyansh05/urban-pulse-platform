#!/usr/bin/env python3
"""
Urban Pulse Platform — Pre-Phase Setup Verifier
Checks: folder structure, credentials, API keys, GCP, AWS, dbt, pre-commit
Run from repo root: python scripts/verify_setup.py
"""

import importlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

# ── Color helpers ─────────────────────────────────────────────────────────────
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
BOLD = "\033[1m"
RESET = "\033[0m"


def ok(msg):
    print(f"  {GREEN}✅ {msg}{RESET}")


def fail(msg):
    print(f"  {RED}❌ {msg}{RESET}")


def warn(msg):
    print(f"  {YELLOW}⚠️  {msg}{RESET}")


def info(msg):
    print(f"  {BLUE}ℹ️  {msg}{RESET}")


def header(msg):
    print(f"\n{BOLD}{msg}{RESET}\n{'─' * 60}")


ERRORS = []
WARNINGS = []


def record_fail(msg):
    ERRORS.append(msg)
    fail(msg)


def record_warn(msg):
    WARNINGS.append(msg)
    warn(msg)


# ── Find repo root ─────────────────────────────────────────────────────────────
def find_repo_root() -> Path:
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / ".git").exists():
            return parent
    return current


ROOT = find_repo_root()
ENV_FILE = ROOT / ".env"

print(f"\n{BOLD}{'═' * 60}")
print("  Urban Pulse Platform — Pre-Phase Verifier")
print(f"{'═' * 60}{RESET}")
info(f"Repo root : {ROOT}")
info(f"Env file  : {ENV_FILE}")

# Load .env
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
    ok(".env file found and loaded")
else:
    record_fail(".env file not found at repo root")

# ══════════════════════════════════════════════════════════════════════════════
# 1. FOLDER STRUCTURE
# ══════════════════════════════════════════════════════════════════════════════
header("1. FOLDER STRUCTURE")

REQUIRED_DIRS = [
    "ingestion/batch",
    "ingestion/streaming",
    "ingestion/schemas",
    "transformation/models/staging",
    "transformation/models/intermediate",
    "transformation/models/marts",
    "transformation/tests",
    "transformation/macros",
    "transformation/seeds",
    "transformation/snapshots",
    "orchestration/dags",
    "orchestration/plugins",
    "orchestration/sensors",
    "infrastructure/terraform/gcp",
    "infrastructure/terraform/aws",
    "infrastructure/terraform/modules",
    "infrastructure/docker",
    "quality/expectations",
    "quality/checkpoints",
    "quality/data_docs",
    "governance/pii_config",
    "governance/lineage",
    "governance/catalog",
    "serving/api",
    "serving/feature_store",
    "monitoring/dashboards",
    "monitoring/alerts",
    "monitoring/metrics",
    "tests/unit",
    "tests/integration",
    "tests/e2e",
    "docs/architecture",
    "docs/decisions",
    "notebooks",
    "scripts",
    "local/duckdb",
    "local/minio_config",
    ".github/workflows",
    ".github/ISSUE_TEMPLATE",
]

REQUIRED_FILES = [
    ".env.example",
    ".gitignore",
    ".pre-commit-config.yaml",
    "Makefile",
    "pyproject.toml",
    "requirements.txt",
    "transformation/dbt_project.yml",
    "docs/decisions/ADR-001-cloud-architecture.md",
    "scripts/verify_api_keys.py",
]

missing_dirs = []
missing_files = []

for d in REQUIRED_DIRS:
    path = ROOT / d
    if path.exists():
        ok(f"dir  {d}")
    else:
        record_fail(f"MISSING dir  {d}")
        missing_dirs.append(d)

for f in REQUIRED_FILES:
    path = ROOT / f
    if path.exists():
        ok(f"file {f}")
    else:
        record_fail(f"MISSING file {f}")
        missing_files.append(f)

# ══════════════════════════════════════════════════════════════════════════════
# 2. ENV VARIABLES
# ══════════════════════════════════════════════════════════════════════════════
header("2. ENVIRONMENT VARIABLES (.env)")

REQUIRED_ENV = {
    "GCP_PROJECT_ID": "GCP project ID",
    "GCP_REGION": "GCP region (e.g. us-central1)",
    "GCP_BUCKET_RAW": "GCS raw bucket name",
    "GOOGLE_APPLICATION_CREDENTIALS": "Path to GCP service account JSON",
    "AWS_ACCESS_KEY_ID": "AWS access key",
    "AWS_SECRET_ACCESS_KEY": "AWS secret key",
    "AWS_REGION": "AWS region (e.g. us-east-1)",
    "AWS_BUCKET_RAW": "S3 raw bucket name",
    "NYC_APP_TOKEN": "NYC Open Data Socrata token",
    "TFL_APP_KEY": "TfL Unified API key",
    "AIRNOW_API_KEY": "AirNow EPA API key",
    "NOAA_CDO_TOKEN": "NOAA CDO token",
}

OPTIONAL_ENV = {
    "DATABRICKS_HOST": "Databricks workspace URL",
    "DATABRICKS_TOKEN": "Databricks personal access token",
    "AIRFLOW_UID": "Airflow user ID (default 50000)",
}

for key, desc in REQUIRED_ENV.items():
    val = os.getenv(key)
    if val:
        # Mask sensitive values
        masked = val[:4] + "****" + val[-2:] if len(val) > 8 else "****"
        ok(f"{key} = {masked}  ({desc})")
    else:
        record_fail(f"{key} not set  ({desc})")

for key, desc in OPTIONAL_ENV.items():
    val = os.getenv(key)
    if val:
        ok(f"{key} set  ({desc})")
    else:
        record_warn(f"{key} not set — optional but recommended  ({desc})")

# ══════════════════════════════════════════════════════════════════════════════
# 3. GCP CREDENTIALS FILE
# ══════════════════════════════════════════════════════════════════════════════
header("3. GCP SERVICE ACCOUNT CREDENTIALS")

gcp_creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if gcp_creds_path:
    creds_file = Path(gcp_creds_path)
    if creds_file.exists():
        ok(f"Credentials file exists: {creds_file}")
        try:
            with open(creds_file) as f:
                creds = json.load(f)
            required_fields = ["type", "project_id", "client_email", "private_key"]
            for field in required_fields:
                if field in creds:
                    ok(f"  Field present: {field} = {str(creds[field])[:40]}...")
                else:
                    record_fail(f"  Missing field in credentials JSON: {field}")
            if creds.get("type") == "service_account":
                ok("  Credential type: service_account ✓")
            else:
                record_fail(f"  Expected service_account, got: {creds.get('type')}")
        except json.JSONDecodeError:
            record_fail("Credentials file is not valid JSON")
    else:
        record_fail(f"Credentials file not found at: {creds_file}")
else:
    record_fail("GOOGLE_APPLICATION_CREDENTIALS not set")

# ══════════════════════════════════════════════════════════════════════════════
# 4. GCP CONNECTIVITY — BigQuery datasets
# ══════════════════════════════════════════════════════════════════════════════
header("4. GCP CONNECTIVITY — BigQuery Datasets")

try:
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT_ID")
    client = bigquery.Client(project=project)

    REQUIRED_DATASETS = ["raw", "staging", "intermediate", "marts", "monitoring"]
    existing = {ds.dataset_id for ds in client.list_datasets()}

    for ds in REQUIRED_DATASETS:
        if ds in existing:
            ok(f"BigQuery dataset exists: {project}.{ds}")
        else:
            record_fail(f"BigQuery dataset MISSING: {project}.{ds}")
            info(f"  Fix: bq mk --dataset --location=US {project}:{ds}")

except ImportError:
    record_fail("google-cloud-bigquery not installed — run: pip install -r requirements.txt")
except Exception as e:
    record_fail(f"GCP BigQuery connection failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 5. GCP CONNECTIVITY — APIs enabled
# ══════════════════════════════════════════════════════════════════════════════
header("5. GCP APIs — Enabled Check")

REQUIRED_APIS = [
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "dataflow.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
]

try:
    result = subprocess.run(
        [
            "gcloud",
            "services",
            "list",
            "--enabled",
            f"--project={os.getenv('GCP_PROJECT_ID')}",
            "--format=value(config.name)",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode == 0:
        enabled = set(result.stdout.strip().split("\n"))
        for api in REQUIRED_APIS:
            if api in enabled:
                ok(f"API enabled: {api}")
            else:
                record_fail(f"API NOT enabled: {api}")
                info(f"  Fix: gcloud services enable {api}")
    else:
        record_warn(
            f"Could not list GCP APIs (gcloud may not be configured): {result.stderr[:100]}"
        )
except FileNotFoundError:
    record_warn("gcloud CLI not found — install from cloud.google.com/sdk")
except subprocess.TimeoutExpired:
    record_warn("gcloud API check timed out — check network connection")

# ══════════════════════════════════════════════════════════════════════════════
# 6. AWS CONNECTIVITY — S3 bucket
# ══════════════════════════════════════════════════════════════════════════════
header("6. AWS CONNECTIVITY — S3 Bucket")

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError

    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )

    bucket = os.getenv("AWS_BUCKET_RAW")
    if bucket:
        try:
            s3.head_bucket(Bucket=bucket)
            ok(f"S3 bucket exists and accessible: {bucket}")

            # Check folder structure
            REQUIRED_PREFIXES = [
                "nyc/311/",
                "nyc/crime/",
                "nyc/transit/",
                "nyc/air_quality/",
                "nyc/weather/",
                "london/transit/",
                "london/air_quality/",
                "_metadata/schemas/",
                "_metadata/manifests/",
            ]
            response = s3.list_objects_v2(Bucket=bucket, Delimiter="/", MaxKeys=100)
            # Just verify bucket is reachable — prefixes are logical
            ok("S3 bucket structure accessible")

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                record_fail(f"S3 bucket not found: {bucket}")
                info(f"  Fix: aws s3api create-bucket --bucket {bucket} --region us-east-1")
            elif error_code == "403":
                record_fail(f"S3 bucket access denied: {bucket} — check IAM permissions")
            else:
                record_fail(f"S3 error: {e}")
    else:
        record_fail("AWS_BUCKET_RAW not set in .env")

except ImportError:
    record_fail("boto3 not installed — run: pip install -r requirements.txt")
except NoCredentialsError:
    record_fail("AWS credentials not found — check AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
except Exception as e:
    record_fail(f"AWS connection failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 7. API KEYS — Live checks
# ══════════════════════════════════════════════════════════════════════════════
header("7. API KEYS — Live Connectivity")

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_http_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.7,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


HTTP = build_http_session()


def api_check(
    name, url, headers=None, params=None, expected_status=200, timeout=15, soft_fail=False
):
    try:
        r = HTTP.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if r.status_code == expected_status:
            ok(f"{name}: HTTP {r.status_code}")
        else:
            msg = f"{name}: HTTP {r.status_code} (expected {expected_status})"
            if soft_fail:
                record_warn(msg)
            else:
                record_fail(msg)
    except requests.exceptions.ConnectionError:
        msg = f"{name}: Connection error — check internet connectivity"
        if soft_fail:
            record_warn(msg)
        else:
            record_fail(msg)
    except requests.exceptions.Timeout:
        msg = f"{name}: Timeout after {timeout}s"
        if soft_fail:
            record_warn(msg)
        else:
            record_fail(msg)
    except Exception as e:
        msg = f"{name}: {e}"
        if soft_fail:
            record_warn(msg)
        else:
            record_fail(msg)


nyc_token = os.getenv("NYC_APP_TOKEN", "")
tfl_key = os.getenv("TFL_APP_KEY", "")
airnow_key = os.getenv("AIRNOW_API_KEY", "")
noaa_token = os.getenv("NOAA_CDO_TOKEN", "")

api_check(
    "NYC Open Data (311)",
    "https://data.cityofnewyork.us/resource/erm2-nwe9.json",
    params={"$limit": 1, "$$app_token": nyc_token},
    timeout=20,
    soft_fail=True,
)
api_check(
    "TfL Unified API",
    "https://api.tfl.gov.uk/Line/victoria/Status",
    params={"app_key": tfl_key},
)
api_check(
    "AirNow EPA",
    "https://www.airnowapi.org/aq/observation/latLong/current/",
    params={
        "format": "application/json",
        "latitude": "40.71",
        "longitude": "-74.00",
        "distance": "25",
        "API_KEY": airnow_key,
    },
)
api_check(
    "Open-Meteo (no key)",
    "https://api.open-meteo.com/v1/forecast",
    params={"latitude": "40.71", "longitude": "-74.00", "current_weather": "true"},
)
api_check(
    "NOAA CDO",
    "https://www.ncdc.noaa.gov/cdo-web/api/v2/datasets?limit=1",
    headers={"token": noaa_token},
)

# ══════════════════════════════════════════════════════════════════════════════
# 8. PYTHON ENVIRONMENT
# ══════════════════════════════════════════════════════════════════════════════
header("8. PYTHON ENVIRONMENT")

REQUIRED_PACKAGES = [
    "requests",
    "dotenv",
    "google.cloud.bigquery",
    "google.cloud.storage",
    "google.cloud.pubsub_v1",
    "boto3",
    "pandas",
    "pyarrow",
    "duckdb",
    "great_expectations",
    "pydantic",
]

for pkg in REQUIRED_PACKAGES:
    try:
        importlib.import_module(pkg)
        ok(f"Package importable: {pkg}")
    except ImportError:
        record_fail(f"Package not importable: {pkg} — run: pip install -r requirements.txt")

# Python version check
py_version = sys.version_info
if py_version >= (3, 11):
    ok(f"Python {py_version.major}.{py_version.minor}.{py_version.micro} (>=3.11 required)")
elif py_version >= (3, 9):
    record_warn(f"Python {py_version.major}.{py_version.minor} — 3.11+ recommended")
else:
    record_fail(f"Python {py_version.major}.{py_version.minor} — 3.11+ required")

# ══════════════════════════════════════════════════════════════════════════════
# 9. DBT SETUP
# ══════════════════════════════════════════════════════════════════════════════
header("9. DBT SETUP")

dbt_project = ROOT / "transformation" / "dbt_project.yml"
if dbt_project.exists():
    ok("transformation/dbt_project.yml exists")
    with open(dbt_project) as f:
        content = f.read()
    for expected in ["urban_pulse", "staging", "intermediate", "marts"]:
        if expected in content:
            ok(f"  dbt_project.yml contains: {expected}")
        else:
            record_fail(f"  dbt_project.yml missing: {expected}")
else:
    record_fail("transformation/dbt_project.yml not found")

dbt_profiles = Path.home() / ".dbt" / "profiles.yml"
if dbt_profiles.exists():
    ok("~/.dbt/profiles.yml exists")
    with open(dbt_profiles) as f:
        content = f.read()
    if "urban_pulse" in content:
        ok("  profiles.yml contains urban_pulse profile")
    else:
        record_fail("  profiles.yml does not contain urban_pulse profile")
else:
    record_fail("~/.dbt/profiles.yml not found")
    info("  Fix: create ~/.dbt/profiles.yml with urban_pulse profile")

# ══════════════════════════════════════════════════════════════════════════════
# 10. GIT SETUP
# ══════════════════════════════════════════════════════════════════════════════
header("10. GIT SETUP")

try:
    if shutil.which("git") is None:
        record_fail("git not found — install git first")
        raise RuntimeError("git command unavailable")

    # Check git is initialized
    result = subprocess.run(
        ["git", "rev-parse", "--is-inside-work-tree"], cwd=ROOT, capture_output=True, text=True
    )
    if result.stdout.strip() == "true":
        ok("Git repository initialized")
    else:
        record_fail("Not inside a git repository")

    # Check remote
    result = subprocess.run(["git", "remote", "-v"], cwd=ROOT, capture_output=True, text=True)
    if "github.com" in result.stdout:
        remote_line = [l for l in result.stdout.split("\n") if "github.com" in l]
        ok(f"GitHub remote configured: {remote_line[0].split()[1] if remote_line else 'found'}")
    else:
        record_fail("No GitHub remote configured")
        info(
            "  Fix: git remote add origin https://github.com/YOUR_USERNAME/urban-pulse-platform.git"
        )

    # Check branch
    result = subprocess.run(
        ["git", "branch", "--show-current"], cwd=ROOT, capture_output=True, text=True
    )
    branch = result.stdout.strip()
    if branch == "main":
        ok("On branch: main")
    else:
        record_warn(f"On branch: {branch} (expected main)")

    # Check .gitignore has sensitive patterns
    gitignore = ROOT / ".gitignore"
    if gitignore.exists():
        content = gitignore.read_text()
        sensitive_patterns = [".env", "*.json", "__pycache__", ".venv", "target/"]
        for pattern in sensitive_patterns:
            if pattern in content:
                ok(f"  .gitignore covers: {pattern}")
            else:
                record_warn(f"  .gitignore missing pattern: {pattern}")
    else:
        record_fail(".gitignore not found")

    # Check pre-commit is installed
    pre_commit_cmd = shutil.which("pre-commit")
    if pre_commit_cmd:
        result = subprocess.run([pre_commit_cmd, "--version"], capture_output=True, text=True)
    else:
        result = subprocess.run(
            [sys.executable, "-m", "pre_commit", "--version"], capture_output=True, text=True
        )

    if result.returncode == 0:
        ok(f"pre-commit installed: {result.stdout.strip()}")
    else:
        record_fail("pre-commit not installed — run: pip install pre-commit && pre-commit install")

    # Check pre-commit hooks are installed in .git
    hooks_file = ROOT / ".git" / "hooks" / "pre-commit"
    if hooks_file.exists():
        ok("pre-commit hooks installed in .git/hooks/")
    else:
        record_warn("pre-commit hooks not installed — run: pre-commit install")

except Exception as e:
    record_fail(f"Git check failed: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# 11. GITIGNORE SAFETY CHECK — nothing sensitive will be committed
# ══════════════════════════════════════════════════════════════════════════════
header("11. SENSITIVE FILE SAFETY CHECK")

SHOULD_NOT_BE_TRACKED = [
    ".env",
    "*.json",  # catches service account keys
]

try:
    result = subprocess.run(
        ["git", "ls-files", "--others", "--cached", "--exclude-standard"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    tracked_files = result.stdout.strip().split("\n")

    # Check .env is not tracked
    if ".env" in tracked_files:
        record_fail(".env IS tracked by git — REMOVE IMMEDIATELY with: git rm --cached .env")
    else:
        ok(".env is not tracked by git ✓")

    # Check no JSON files in root (service account keys)
    json_in_root = [f for f in tracked_files if f.endswith(".json") and "/" not in f]
    if json_in_root:
        record_fail(f"JSON files tracked in root (possible key leak): {json_in_root}")
    else:
        ok("No JSON credential files tracked in repo root ✓")

    # Check no credential files anywhere obvious
    key_patterns = ["key.json", "credentials.json", "service-account"]
    suspicious = [f for f in tracked_files if any(p in f.lower() for p in key_patterns)]
    if suspicious:
        record_fail(f"Possible credential files tracked: {suspicious}")
    else:
        ok("No suspicious credential filenames tracked ✓")

except Exception as e:
    record_warn(f"Could not check git tracked files: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{BOLD}{'═' * 60}")
print("  VERIFICATION SUMMARY")
print(f"{'═' * 60}{RESET}")

if not ERRORS and not WARNINGS:
    print(
        f"\n{GREEN}{BOLD}  🎉 ALL CHECKS PASSED — Pre-phase complete. Ready for Phase 0.{RESET}\n"
    )
elif not ERRORS and WARNINGS:
    print(f"\n{YELLOW}{BOLD}  ⚠️  PASSED WITH WARNINGS ({len(WARNINGS)} warning(s)){RESET}")
    print(f"{YELLOW}  Warnings (non-blocking):{RESET}")
    for w in WARNINGS:
        print(f"    • {w}")
    print(f"\n{GREEN}  No blocking errors. Ready to proceed to Phase 0.{RESET}\n")
else:
    print(f"\n{RED}{BOLD}  ❌ {len(ERRORS)} BLOCKING ERROR(S) — Fix before proceeding{RESET}")
    print(f"{RED}  Errors to fix:{RESET}")
    for e in ERRORS:
        print(f"    • {e}")
    if WARNINGS:
        print(f"\n{YELLOW}  Warnings (non-blocking):{RESET}")
        for w in WARNINGS:
            print(f"    • {w}")
    print(f"\n{RED}  Fix all errors above, then re-run: python scripts/verify_setup.py{RESET}\n")
    sys.exit(1)
