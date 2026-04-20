"""
dbt Assets - Data Transformation Models

This module defines Dagster assets that run dbt models to transform
raw data from BigQuery into analytics-ready tables.

The dbt project includes:
- Staging models: Clean raw data (stg_*)
- Intermediate models: Build engagement metrics (int_*)
- Marts models: Business-ready reports (manager_team_detail_pdf_makecom, etc.)
"""

from pathlib import Path
import sys
from dagster import AssetExecutionContext, asset, Output
from dagster_dbt import DbtCliResource
import subprocess
import logging

from dagster_project.assets.airbyte_assets import airbyte_assets as _airbyte_asset_defs

logger = logging.getLogger(__name__)

# Path to dbt project (relative to this file)
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt" / "realpars_community"

# Collect all Airbyte asset keys so dbt waits for all syncs to complete
_airbyte_dep_keys = []
for _asset_def in _airbyte_asset_defs:
    _airbyte_dep_keys.extend(_asset_def.keys)


def _validate_dbt_environment(context: AssetExecutionContext) -> None:
    """Validate the env vars dbt needs before spawning the subprocess."""
    from configs import config

    required_env_values = {
        "PROJECT_ID": config.PROJECT_ID,
        "BQ_RAW_DATASET": config.BQ_RAW_DATASET,
        "GOOGLE_APPLICATION_CREDENTIALS": config.GOOGLE_APPLICATION_CREDENTIALS,
    }

    optional_env_values = {
        "BQ_STG_CLEAN_DATASET": config.BQ_STG_CLEAN_DATASET,
        "BQ_STG_BUSINESS_RELATIONSHIPS_DATASET": config.BQ_STG_BUSINESS_RELATIONSHIPS_DATASET,
        "BQ_STG_WEEKLY_REPORTS_DATASET": config.BQ_STG_WEEKLY_REPORTS_DATASET,
        "BQ_STG_TRANSFORMED_DATASET": config.BQ_STG_TRANSFORMED_DATASET,
    }

    missing_vars = [name for name, value in required_env_values.items() if not value]
    for name, value in required_env_values.items():
        status = "set" if value else "missing"
        context.log.info(f"dbt env check: required {name}={status}")

    for name, value in optional_env_values.items():
        status = "set" if value else "missing"
        context.log.info(f"dbt env check: optional {name}={status}")

    if missing_vars:
        missing_vars_str = ", ".join(missing_vars)
        raise Exception(
            f"dbt environment validation failed. Missing required env vars: {missing_vars_str}. "
            "In Dagster Cloud branch deployments, ensure these vars are configured for the deployment."
        )

    credentials_path = Path(str(required_env_values["GOOGLE_APPLICATION_CREDENTIALS"]))
    if not credentials_path.exists():
        raise Exception(
            "dbt environment validation failed. "
            f"GOOGLE_APPLICATION_CREDENTIALS points to a file that does not exist: {credentials_path}"
        )

    context.log.info(f"dbt env check: credentials_path={credentials_path}")


@asset(
    name="realpars_dbt_models",
    deps=_airbyte_dep_keys,
    description="Run all dbt models to transform Circle.so data",
    group_name="dbt_transformations",
)
def realpars_dbt_models(context: AssetExecutionContext) -> Output:
    """
    Run all dbt models to transform Circle.so data.
    
    This asset depends on Airbyte assets completing first (raw data must exist).
    
    dbt models run in this order:
    1. Staging: Clean raw data (stg_*)
    2. Intermediate: Calculate engagement metrics (int_*)
    3. Marts: Build business reports for Make.com
    """
    context.log.info("Starting dbt build process...")
    context.log.info(f"dbt project directory: {DBT_PROJECT_DIR}")
    context.log.info(f"Dagster Python executable: {sys.executable}")
    _validate_dbt_environment(context)
    
    # Run dbt wrapper script (which loads .env and runs dbt)
    run_dbt_script = DBT_PROJECT_DIR / "run_dbt.py"
    
    try:
        result = subprocess.run(
            [sys.executable, str(run_dbt_script), "build"],
            cwd=str(DBT_PROJECT_DIR),  # Run from dbt project directory
            capture_output=True,
            text=True,
            timeout=1800,  # 30 minute timeout
        )
        
        # Log dbt output (both stdout and stderr)
        if result.stdout:
            for line in result.stdout.split('\n'):
                if line.strip():
                    context.log.info(line)
        
        if result.stderr:
            for line in result.stderr.split('\n'):
                if line.strip() and not line.startswith('✅') and not line.startswith('⚠️'):
                    context.log.warning(f"[stderr] {line}")
        
        if result.returncode != 0:
            context.log.error(f"❌ dbt build failed with return code {result.returncode}")
            context.log.error("=" * 80)
            context.log.error("Full stdout output:")
            context.log.error(result.stdout if result.stdout else "(no stdout)")
            context.log.error("=" * 80)
            context.log.error("Full stderr output:")
            context.log.error(result.stderr if result.stderr else "(no stderr)")
            context.log.error("=" * 80)
            raise Exception(f"dbt build failed with return code {result.returncode}. Check logs above for details.")
        
        context.log.info("✓ dbt build completed successfully")
        
        return Output(
            value={"status": "success", "return_code": result.returncode},
            metadata={
                "dbt_project": str(DBT_PROJECT_DIR),
                "command": "dbt build",
            }
        )
        
    except subprocess.TimeoutExpired:
        context.log.error("dbt build timed out after 30 minutes")
        raise
    except Exception as e:
        context.log.error(f"Failed to run dbt: {e}")
        raise
