"""
dbt Resource Configuration

This module configures the dbt CLI resource for running dbt commands.
"""

from pathlib import Path
from dagster_dbt import DbtCliResource
import logging

logger = logging.getLogger(__name__)

# Path to dbt project directory
DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt" / "realpars_community"


def get_dbt_resource() -> DbtCliResource:
    """
    Create and configure dbt CLI resource.
    
    Returns:
        Configured DbtCliResource instance
    """
    
    if not DBT_PROJECT_DIR.exists():
        raise ValueError(f"dbt project directory not found: {DBT_PROJECT_DIR}")
    
    dbt_project_yml = DBT_PROJECT_DIR / "dbt_project.yml"
    if not dbt_project_yml.exists():
        raise ValueError(f"dbt_project.yml not found in: {DBT_PROJECT_DIR}")
    
    logger.info(f"Configuring dbt resource for project: {DBT_PROJECT_DIR}")
    
    return DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
    )


# Create the resource instance
dbt_resource = get_dbt_resource()
