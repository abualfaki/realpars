"""Airbyte resource for syncing data from Circle.so to BigQuery."""

import logging

from dagster_airbyte import AirbyteCloudResource

from configs.config import (
    AIRBYTE_CLIENT_ID,
    AIRBYTE_CLIENT_SECRET,
)

logger = logging.getLogger(__name__)


def _require_config_value(value: str | None, env_var: str) -> str:
    if not value:
        raise ValueError(f"Set {env_var} in your environment to use the Airbyte resource.")
    return value


airbyte_resource = AirbyteCloudResource(
    client_id=_require_config_value(AIRBYTE_CLIENT_ID, "AIRBYTE_CLIENT_ID"),
    client_secret=_require_config_value(AIRBYTE_CLIENT_SECRET, "AIRBYTE_CLIENT_SECRET"),
)
