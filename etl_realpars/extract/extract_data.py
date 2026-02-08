import os
import sys
import requests
import airbyte as ab
import pandas as pd

import logging
from typing import Any

# Import config to load secrets
import etl_realpars.configs.config as config

headers = {
    "Authorization": f"Bearer {config.CIRCLE_CI_ADMIN_V2_KEY}",
    "Accept": "application/json"
}

params = {
    "page": 1,
    "per_page": 100
}

response = requests.get(config.CIRCLE_COMMUNITY_MEMBERS_ENDPOINT, headers = headers, params = params, timeout = 30)

print("Status:", response.status_code)
print(response.json())
'''
available_sources = ab.get_available_connectors()

# configure source and destination connectors
data_source: ab.Source = ab.get_source("source-circleci")

destination_db: ab.Destination = ab.get_source("destination-bigquery")

for source in available_sources:
    print(source)


class ExtractData:

    def __init__(self, source: str, destination: str):
        self.source = source
        self.destination = destination

    def extract_these_rows(paylopad: dict) -> list(dict):

        rows = []

        for recird in 

'''