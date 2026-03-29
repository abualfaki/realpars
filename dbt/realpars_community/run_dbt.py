#!/usr/bin/env python3
"""
DBT Wrapper Script for RealPars Project

This script ensures that environment variables from .env are loaded
before running any dbt commands.

Usage:
    python run_dbt.py debug
    python run_dbt.py run --select stg_community_members
    python run_dbt.py test --select clean_communtity
    python run_dbt.py build
    python run_dbt.py docs generate
    python run_dbt.py docs serve

Author: RealPars ETL Pipeline
"""
import sys
import subprocess
import os
from pathlib import Path

def main():
    # Get project root (2 levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # Import config to trigger .env loading
    try:
        from configs import config
    except ImportError as e:
        print("❌ Error: Could not import config module")
        print(f"   Make sure you're in the project root: {project_root}")
        print(f"   Error: {e}")
        sys.exit(1)

    # Print loaded environment variables
    print("=" * 70)
    print("🔧 Environment Variables Loaded from .env")
    print("=" * 70)
    
    env_vars = {
        "PROJECT_ID": config.PROJECT_ID,
        "BQ_RAW_DATASET": config.BQ_RAW_DATASET,
        "BQ_STG_CLEAN_DATASET": config.BQ_STG_CLEAN_DATASET,
        "BQ_STG_BUSINESS_DATASET": config.BQ_STG_BUSINESS_RELATIONSHIPS_DATASET,
        "BQ_STG_WEEKLY_REPORTS_DATASET": config.BQ_STG_WEEKLY_REPORTS_DATASET,
        "BQ_STG_TRANSFORMED_DATASET": config.BQ_STG_TRANSFORMED_DATASET,
        "GOOGLE_APPLICATION_CREDENTIALS": config.GOOGLE_APPLICATION_CREDENTIALS,
    }
    
    for key, value in env_vars.items():
        if value:
            # Truncate long values for display
            display_value = value if len(str(value)) < 50 else f"{str(value)[:47]}..."
            print(f"✅ {key:35s} = {display_value}")
        else:
            print(f"⚠️  {key:35s} = NOT SET")
    
    print("=" * 70)
    print()

    # Check if dbt command was provided
    if len(sys.argv) < 2:
        print("❌ Error: No dbt command provided\n")
        print("Usage Examples:")
        print("  python run_dbt.py debug")
        print("  python run_dbt.py run --select stg_community_members")
        print("  python run_dbt.py test")
        print("  python run_dbt.py build --select staging.*")
        print("  python run_dbt.py docs generate")
        print("  python run_dbt.py docs serve")
        sys.exit(1)

    # Build dbt command from arguments
    dbt_command = ['dbt'] + sys.argv[1:] + ['--profiles-dir', str(Path(__file__).parent)]
    
    print(f"🚀 Running: {' '.join(dbt_command)}")
    print()

    # Run dbt with inherited environment
    try:
        result = subprocess.run(dbt_command, check=False)
        sys.exit(result.returncode)
    except FileNotFoundError:
        print("❌ Error: 'dbt' command not found")
        print("   Make sure dbt is installed: pip install dbt-bigquery")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
        sys.exit(130)

if __name__ == "__main__":
    main()