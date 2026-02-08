from __future__ import annotations
from dotenv import load_dotenv
load_dotenv(override=True) #Load Latest .env varibles

import os
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s\n\n')
logger = logging.getLogger("config")
logger.setLevel(logging.INFO)

# ============================================================================
# SAFE ENV VAR PARSING - Prevents container crashes from missing variables
# ============================================================================

def _safe_int(env_var_name: str) -> int | None:

    env_value = os.getenv(env_var_name)
    
    if env_value is None:
        logger.warning(f"⚠️ Environment variable {env_var_name} not set. Make sure to set it in the .env file or github secrets")
    
    try:
        return int(env_value)
        
    except (ValueError, TypeError) as e:
        logger.error(f"❌ Environment variable {env_var_name} has invalid value '{env_value}'. Expected an integer. Error: {e}")
        raise ValueError(f"{env_var_name} must be an integer, got: {env_value}") from e


def _url_parse(base_url: str, endpoint: str) -> str:
    if base_url.endswith("/") and endpoint.startswith("/"):
        return base_url[:-1] + endpoint
    elif not base_url.endswith("/") and not endpoint.startswith("/"):
        return base_url + "/" + endpoint
    else:
        return base_url + endpoint


# Set ACTIVE_ENVIRONMENT variable to "production" or "development"
try:
    if os.getenv("ACTIVE_ENVIRONMENT") is None:
        logger.warning("⚠️ Environment variable ACTIVE_ENVIRONMENT not set. Defaulting to 'dev'")

    ACTIVE_ENVIRONMENT = os.getenv("ACTIVE_ENVIRONMENT", "dev").lower() # dev environment by default

    if ACTIVE_ENVIRONMENT == "prod":
        ACTIVE_BUCKET = os.getenv("GCP_PROD_BUCKET")

    elif ACTIVE_ENVIRONMENT == "dev":
        ACTIVE_BUCKET = os.getenv("GCP_DEV_BUCKET")
except Exception as e:
    logger.error(f"❌ Error setting ACTIVE_ENVIRONMENT or ACTIVE_BUCKET: {e}")
    raise e


# Google Cloud Project ID
try:
    PROJECT_ID = os.getenv("PROJECT_ID")
    
    if PROJECT_ID is None:
        logger.warning("⚠️ Environment variable PROJECT_ID not set. Make sure to set it in the .env file or github secrets")
except Exception as e:
    logger.error(f"❌ Error setting PROJECT_ID: {e}")
    raise e


# Circle.so API Keys
try:
    CIRCLE_CI_ADMIN_V2_KEY = os.getenv("CIRCLE_CI_ADMIN_V2_KEY")
    CIRCLE_CI_ADMIN_V1_KEY = os.getenv("CIRCLE_CI_ADMIN_V1_KEY")
    
    if CIRCLE_CI_ADMIN_V2_KEY is None or CIRCLE_CI_ADMIN_V1_KEY is None:
        logger.warning("⚠️ One or both Circle.so API keys are not set. Make sure to set them in the .env file or github secrets")
except Exception as e:
    logger.error(f"❌ Error setting Circle.so API keys: {e}")
    raise e

# Fully Parsed Circle.so API endpoints
CIRCLE_COMMUNITY_MEMBERS_ENDPOINT = _url_parse(os.getenv("CIRCLE_API_BASE_URL", ""), os.getenv("CIRCLE_ADMIN_V2_COMMUNITY_MEMBERS_ENDPOINT", ""))


# BigQuery API Key
try:
    BIGQUERY_API_KEY = os.getenv("BIGQUERY_API_KEY")
    if BIGQUERY_API_KEY is None:
        logger.warning("⚠️ Environment variable BIGQUERY_API_KEY not set. Make sure to set it in the .env file or github secrets")
except Exception as e:
    logger.error(f"❌ Error setting BIGQUERY_API_KEY: {e}")
    raise e