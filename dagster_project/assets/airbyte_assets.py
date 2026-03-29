"""
Airbyte Assets - Circle.so Data Extraction

This module defines Dagster assets that sync data from Circle.so API to BigQuery
using Airbyte connections.

Each connection syncs specific tables:
- community_members: Member data
- comments: Post comments
- posts: Community posts
- events: Live classes and attendees
- member_tags: Member tagging data
"""

from dagster import AssetExecutionContext, asset, Output
from dagster_airbyte import AirbyteResource, build_airbyte_assets
from typing import Dict, Any, List
import logging

from configs.config import AIRBYTE_CONNECTION_IDS

logger = logging.getLogger(__name__)

# Build Airbyte assets for each configured connection
# We need to call build_airbyte_assets separately for each connection
airbyte_assets_list = []

# Mapping of connection names to their table outputs
CONNECTION_TABLE_MAP = {
    "community_members": ["community_members", "community_members_history"],
    "course_lessons_completed": ["course_lesson_completed"],
    "course_completed": ["course_completed"],
    "events": ["events_list", "event_attendees"],
    "member_tags": ["member_tags_list"],
    "post_comments": ["post_comment_created"],
    "post_comment_liked": ["post_comment_liked"],
    "post_liked": ["post_liked"],
}

# Build assets for each configured connection
for connection_name, connection_id in AIRBYTE_CONNECTION_IDS.items():
    if connection_id is not None:
        # Get the tables for this connection
        tables = CONNECTION_TABLE_MAP.get(connection_name, [connection_name])
        
        # Build asset for this connection
        assets = build_airbyte_assets(
            connection_id=connection_id,
            destination_tables=tables,
            asset_key_prefix=["raw", connection_name],
            group_name="airbyte_to_bigquery_sync",
        )
        airbyte_assets_list.extend(assets)

# Export the list of all Airbyte assets
airbyte_assets = airbyte_assets_list

