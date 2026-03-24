#!/usr/bin/env python3
"""
Loads the managers_not_joined CSV into BigQuery as a native table.
Uses the same env-loading pattern as run_dbt.py.

Usage:
    python load_managers_csv.py
"""
import sys
import subprocess
from pathlib import Path

# Mirror run_dbt.py: get project root (3 levels up from this file)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from configs import config  # loads .env and sets GOOGLE_APPLICATION_CREDENTIALS

csv_path = project_root / "data" / "managers that havn't joined.csv"

print(f"📂 Loading: {csv_path}")
print(f"🎯 Destination: circle_community_raw_datasets.managers_not_joined\n")

result = subprocess.run([
    "bq", "--project_id=circle-analytics-468017", "load",
    "--replace", "--autodetect",
    "--source_format=CSV", "--skip_leading_rows=1", "--location=US",
    "circle_community_raw_datasets.managers_not_joined",
    str(csv_path),
])

if result.returncode == 0:
    print("\n✅ Load succeeded!")
else:
    print(f"\n❌ Load failed (exit code {result.returncode})")

sys.exit(result.returncode)
