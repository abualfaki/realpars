import os
import sys
import requests
import airbyte as ab
import pandas as pd
from google.cloud import bigquery

import logging
from typing import Any

from data_warehouse_config.tables.COMMUNITY_MEMBERS import create_community_members_table

# Import config to load secrets
import configs.config as config


def extract_circle_community_members() -> None:
    """
    Extracts community member data from Circle.so API, transforms it into a DataFrame, and loads it into BigQuery.
    """
    create_community_members_table()  # Ensure the BigQuery table exists before loading data

    headers = {
        "Authorization": f"Bearer {config.CIRCLE_CI_ADMIN_V2_KEY}",
        "Accept": "application/json"
    }

    params = {
        "page": 1,
        "per_page": 100
    }

    all_records = []

    # Paginate through all pages and collect records
    while True:
        response = requests.get(
            config.CIRCLE_COMMUNITY_MEMBERS_ENDPOINT,
            headers=headers,
            params=params,
            timeout=30,
        )

        print("Status:", response.status_code, "page:", params["page"])

        if response.status_code != 200:
            raise RuntimeError(f"Request failed: {response.status_code} - {response.text}")

        data = response.json()
        page_records = data.get("records", [])
        all_records.extend(page_records)

        # Stop when API indicates no next page or when returned records are empty
        if not data.get("has_next_page", False) or len(page_records) == 0:
            break

        params["page"] = params.get("page", 1) + 1


    # Helper to pull desired fields safely
    def extract_member_fields(rec: dict) -> dict:
        gm = rec.get("gamification_stats") or {}
        return {
            "community_member_id": gm.get("community_member_id"),
            "member_id": rec.get("id"),
            "email": rec.get("email"),
            "first_name": rec.get("first_name"),
            "last_name": rec.get("last_name"),
            "created_at": rec.get("created_at"),
            "accepted_invitation": rec.get("accepted_invitation"),
            "posts_count": rec.get("posts_count"),
            "comments_count": rec.get("comments_count"),
            "member_tags": rec.get("member_tags"),
        }


    rows = [extract_member_fields(r) for r in all_records]

    df = pd.DataFrame(rows)

    print(f"Total records fetched: {len(all_records)}")
    print(df.head(5).to_dict(orient="records"))

    client = bigquery.Client()
    dataset_id = config.BQ_DATASET_ID
    community_table_id = config.COMMUNITY_MEMBERS_TABLE
    table_ref = f"{client.project}.{dataset_id}.{community_table_id}"

    # Get the actual table object
    table = client.get_table(table_ref)

    # Insert rows into BigQuery
    errors = client.insert_rows_from_dataframe(table, df)

    if any(errors):  # Check if any chunk has actual errors
        print(f"Errors inserting rows: {errors}")
    else:
        print(f"Successfully inserted {len(df)} rows into {table_ref}")
