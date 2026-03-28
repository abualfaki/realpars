"""
Assets Module

Contains all Dagster assets:
- airbyte_assets: Circle.so data extraction
- dbt_assets: Data transformation
- make_dot_com_trigger: Email automation trigger
"""

from . import airbyte_assets, dbt_assets, make_dot_com_trigger

__all__ = ["airbyte_assets", "dbt_assets", "make_dot_com_trigger"]
