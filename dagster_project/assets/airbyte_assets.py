"""Airbyte assets backed by Dagster's AirbyteCloudWorkspace resource."""

from dagster_airbyte import build_airbyte_assets_definitions

from configs.config import AIRBYTE_CONNECTION_IDS
from dagster_project.resources.airbyte_resources import airbyte_resource


def _build_connection_selector():
    configured_ids = {value for value in AIRBYTE_CONNECTION_IDS.values() if value}
    configured_names = {name for name, value in AIRBYTE_CONNECTION_IDS.items() if value}

    if not configured_ids:
        return None

    def _selector(connection) -> bool:
        return connection.id in configured_ids or connection.name in configured_names

    return _selector


airbyte_assets = build_airbyte_assets_definitions(
    workspace=airbyte_resource,
    connection_selector_fn=_build_connection_selector(),
)

