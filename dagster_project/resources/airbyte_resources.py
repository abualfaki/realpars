"""Airbyte resource backed by Dagster's AirbyteCloudWorkspace."""

import logging

from dagster_airbyte import AirbyteCloudWorkspace

from configs.config import (
    AIRBYTE_CLIENT_ID,
    AIRBYTE_CLIENT_SECRET,
    AIRBYTE_WORKSPACE_ID,
)

logger = logging.getLogger(__name__)


def _require_config_value(value: str | None, env_var: str) -> str:
    if not value:
        raise ValueError(f"Set {env_var} in your environment to use the Airbyte workspace resource.")
    return value


airbyte_workspace = AirbyteCloudWorkspace(
    workspace_id=_require_config_value(AIRBYTE_WORKSPACE_ID, "AIRBYTE_WORKSPACE_ID"),
    client_id=_require_config_value(AIRBYTE_CLIENT_ID, "AIRBYTE_CLIENT_ID"),
    client_secret=_require_config_value(AIRBYTE_CLIENT_SECRET, "AIRBYTE_CLIENT_SECRET"),
)

# Export under the previous name so existing imports continue to work.
airbyte_resource = airbyte_workspace
