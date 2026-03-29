from setuptools import find_packages, setup

setup(
    name="realpars_analytics_pipeline",
    packages=find_packages(),
    install_requires=[
        "dagster>=1.7.0",
        "dagster-airbyte>=0.23.0",
        "dagster-dbt>=0.23.0",
        "dagster-gcp>=0.23.0",
        "dagster-cloud",
        "dbt-bigquery>=1.0.0",
        "requests>=2.31.0",
        "python-dotenv>=1.0.0",
        "google-cloud-bigquery>=3.11.0",
        "google-cloud-storage>=2.10.0",
        "pandas>=2.0.0",
    ],
)
