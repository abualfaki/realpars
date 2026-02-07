import os
import airbyte as ab
import pandas as pd

import logging 
available_sources = ab.get_available_connectors()


# configure source and destination connectors
data_source = ab.get_source("source-circleci")
destination_db = ab.get_source("destination-bigquery")
for source in available_sources:
    print(source)