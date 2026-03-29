from __future__ import annotations
from dotenv import load_dotenv
from pathlib import Path

import os
import logging

# Load .env from project root
project_root = Path(__file__).parent.parent
env_path = project_root / ".env"

print(f"Loading environment from: {env_path}")
load_dotenv(env_path, override=True) #Load Latest .env varibles

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


# Google Application Credentials (Service Account Key)
try:
    GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    
    if GOOGLE_APPLICATION_CREDENTIALS is None:
        logger.warning("⚠️ Environment variable GOOGLE_APPLICATION_CREDENTIALS not set. Make sure to set it in the .env file or github secrets")
    elif GOOGLE_APPLICATION_CREDENTIALS.strip().startswith('{'):
        # JSON content provided directly (e.g. Dagster Cloud env var) — write to temp file
        import tempfile
        tmp_creds = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        tmp_creds.write(GOOGLE_APPLICATION_CREDENTIALS)
        tmp_creds.close()
        GOOGLE_APPLICATION_CREDENTIALS = tmp_creds.name
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
        logger.info(f"✅ Google credentials written to temp file: {GOOGLE_APPLICATION_CREDENTIALS}")
    else:
        # File path provided (local development)
        if not GOOGLE_APPLICATION_CREDENTIALS.startswith('/'):
            GOOGLE_APPLICATION_CREDENTIALS = str(project_root / GOOGLE_APPLICATION_CREDENTIALS)
            logger.info(f"✅ Converted credentials path to absolute: {GOOGLE_APPLICATION_CREDENTIALS}")
        
        if not os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
            logger.error(f"❌ Google credentials file not found at: {GOOGLE_APPLICATION_CREDENTIALS}")
        else:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = GOOGLE_APPLICATION_CREDENTIALS
            logger.info(f"✅ Google credentials file found and set: {GOOGLE_APPLICATION_CREDENTIALS}")
            
except Exception as e:
    logger.error(f"❌ Error setting GOOGLE_APPLICATION_CREDENTIALS: {e}")
    raise e


# Circle.so API Keys
try:
    CIRCLE_DATA_API_TOKEN = os.getenv("CIRCLE_DATA_API_TOKEN")
    CIRCLE_CI_ADMIN_V2_KEY = os.getenv("CIRCLE_CI_ADMIN_V2_KEY")
    CIRCLE_CI_ADMIN_V1_KEY = os.getenv("CIRCLE_CI_ADMIN_V1_KEY")
    
    if CIRCLE_CI_ADMIN_V2_KEY is None or CIRCLE_CI_ADMIN_V1_KEY is None or CIRCLE_DATA_API_TOKEN is None:
        logger.warning("⚠️ One or more Circle.so API keys are not set. Make sure to set them in the .env file or github secrets")
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


# BigQuery(BQ) Dataset and Table Names
try:
    BQ_RAW_DATASET = os.getenv("BQ_RAW_DATASET")
    BQ_STG_CLEAN_DATASET = os.getenv("BQ_STG_CLEAN_DATASET")
    BQ_STG_BUSINESS_RELATIONSHIPS_DATASET = os.getenv("BQ_STG_BUSINESS_RELATIONSHIPS_DATASET")
    BQ_STG_WEEKLY_REPORTS_DATASET = os.getenv("BQ_STG_WEEKLY_REPORTS_DATASET")
    BQ_STG_TRANSFORMED_DATASET = os.getenv("BQ_STG_TRANSFORMED_DATASET")
    
    # Legacy individual table name variables (for backward compatibility)
    RAW_COMMUNITY_MEMBERS_TABLE = os.getenv("RAW_COMMUNITY_MEMBERS_TABLE")
    RAW_COMMUNITY_MEMBERS_HISTORY_TABLE = os.getenv("RAW_COMMUNITY_MEMBERS_HISTORY_TABLE")
    RAW_COURSE_COMPLETED_TABLE = os.getenv("RAW_COURSE_COMPLETED_TABLE")
    RAW_MEMBER_TAGS_TABLE = os.getenv("RAW_MEMBER_TAGS_TABLE")
    RAW_COMMENTS_TABLE = os.getenv("RAW_COMMENTS_TABLE")
    RAW_POSTS_TABLE = os.getenv("RAW_POSTS_TABLE")
    RAW_EVENTS_ATTENDEES_TABLE = os.getenv("RAW_EVENTS_ATTENDEES_TABLE")
    RAW_EVENTS_LIST_TABLE = os.getenv("RAW_EVENTS_LIST_TABLE")

    # Clean Staging Table Names
    STG_CLEAN_COMMUNITY_MEMBERS_TABLE = os.getenv("STG_CLEAN_COMMUNITY_MEMBERS_TABLE")
    STG_CLEAN_COMMENTS_TABLE = os.getenv("STG_CLEAN_COMMENTS_TABLE")
    STG_CLEAN_COURSE_COMPLETED_TABLE = os.getenv("STG_CLEAN_COURSE_COMPLETED_TABLE")
    STG_CLEAN_POSTS_TABLE = os.getenv("STG_CLEAN_POSTS_TABLE")
    STG_CLEAN_MEMBER_TAGS_TABLE = os.getenv("STG_CLEAN_MEMBER_TAGS_TABLE")
    STG_CLEAN_EVENTS_ATTENDEES_TABLE = os.getenv("STG_CLEAN_EVENTS_ATTENDEES_TABLE")
    STG_CLEAN_EVENTS_LIST_TABLE = os.getenv("STG_CLEAN_EVENTS_LIST_TABLE")
    STG_CLEAN_COURSE_COMPLETED_TABLE = os.getenv("STG_CLEAN_COURSE_COMPLETED_TABLE")

    # Business Relationships Table
    BQ_STG_BUSINESS_LIST_TABLE=os.getenv("BQ_STG_BUSINESS_LIST_TABLE")
    BQ_STG_BUSINESS_RELATIONSHIPS_TABLE=os.getenv("BQ_STG_BUSINESS_RELATIONSHIPS_TABLE")


    # BQ_STG_WEEKLY_REPORTS_DATASET Table Names
    STG_WEEKLY_REPORTS_COURSE_COMPLETION_TABLE = os.getenv("STG_WEEKLY_REPORTS_COURSE_COMPLETION_TABLE")

    # Transformed Table Names
    STG_TRANSFORMED_COURSE_COMPLETED_TABLE = os.getenv("STG_TRANSFORMED_COURSE_COMPLETED_TABLE")

    # Log warnings for missing tables
    if BQ_RAW_DATASET is None:
        logger.warning("⚠️ BIGQUERY_RAW_DATASET not set. Make sure to set it in the .env file")
    if BQ_STG_CLEAN_DATASET is None:
        logger.warning("⚠️ BIGQUERY_STG_CLEAN_DATASET not set. Make sure to set it in the .env file")
    if BQ_STG_TRANSFORMED_DATASET is None:
        logger.warning("⚠️ BIGQUERY_STG_TRANSFORMED_DATASET not set. Make sure to set it in the .env file")
    if BQ_STG_BUSINESS_RELATIONSHIPS_DATASET is None:
        logger.warning("⚠️ BIGQUERY_STG_BUSINESS_RELATIONSHIPS_DATASET not set. Make sure to set it in the .env file")
        
except Exception as e:
    logger.error(f"❌ Error setting BigQuery dataset or table names: {e}")
    raise e


# Airbyte Configuration
try:
    # Airbyte Cloud API URL
    AIRBYTE_API_URL = os.getenv("AIRBYTE_API_URL", "https://api.airbyte.com/v1")
    
    # OAuth2 Credentials (RECOMMENDED - more secure)
    AIRBYTE_CLIENT_ID = os.getenv("AIRBYTE_CLIENT_ID")
    AIRBYTE_CLIENT_SECRET = os.getenv("AIRBYTE_CLIENT_SECRET")
    
    # Legacy Bearer Token (still supported)
    AIRBYTE_API_TOKEN = os.getenv("AIRBYTE_API_TOKEN")
    AIRBYTE_WORKSPACE_ID = os.getenv("AIRBYTE_WORKSPACE_ID")
    
    # Path to token file as fallback
    AIRBYTE_TOKEN_FILE = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 
        "secrets", 
        "airbytetoken_2026_02_19.txt"
    )
    
    # Load token from file if environment variable not set
    if AIRBYTE_API_TOKEN is None and os.path.exists(AIRBYTE_TOKEN_FILE):
        with open(AIRBYTE_TOKEN_FILE, 'r') as f:
            AIRBYTE_API_TOKEN = f.read().strip()
        logger.info("✅ Loaded Airbyte token from file")
    
    # Determine authentication method (matches priority in airbyte_factory.py)
    if AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET:
        logger.info("✅ Using OAuth2 authentication (Client ID/Secret)")
    elif AIRBYTE_API_TOKEN:
        logger.info("✅ Using Bearer token authentication")
    else:
        logger.warning("⚠️ No Airbyte authentication configured. Set either:")
        logger.warning("   - AIRBYTE_API_TOKEN (from Airbyte Cloud)")
        logger.warning("   - AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET")
    
    # Multiple Airbyte Connection IDs (one per data source/table)
    AIRBYTE_CONNECTION_IDS = {
        "community_members": os.getenv("AIRBYTE_CONNECTION_ID_COMMUNITY_MEMBERS_TABLE"),
        "course_lessons_completed": os.getenv("AIRBYTE_CONNECTION_ID_COURSE_LESSONS_COMPLETED_TABLE"),
        "course_completed": os.getenv("AIRBYTE_CONNECTION_ID_COURSE_COMPLETED_TABLE"),
        "events": os.getenv("AIRBYTE_CONNECTION_ID_EVENTS_LISTS_AND_EVENTS_ATTENDEES"),
        "member_tags": os.getenv("AIRBYTE_CONNECTION_ID_MEMBER_TAGS_TABLE"),
        "post_comments": os.getenv("AIRBYTE_CONNECTION_ID_POST_COMMENTS_TABLE"),
        "post_comment_liked": os.getenv("AIRBYTE_CONNECTION_ID_POST_COMMENTS_LIKED_TABLE"),
        "post_liked": os.getenv("AIRBYTE_CONNECTION_ID_POST_LIKED_TABLE"),
    }
    
    # Log any missing connection IDs
    missing_connections = [key for key, value in AIRBYTE_CONNECTION_IDS.items() if value is None]
    if missing_connections:
        logger.warning(f"⚠️ Missing Airbyte connection IDs for: {', '.join(missing_connections)}")
    
    # Legacy single connection ID (for backward compatibility)
    AIRBYTE_CONNECTION_ID = os.getenv("AIRBYTE_CONNECTION_ID") or AIRBYTE_CONNECTION_IDS.get("community_members")
    
    # Mapping between connection names and their corresponding BigQuery table names
    # This makes it easy to know which table is populated by which Airbyte connection
    CONNECTION_TO_TABLE_MAP = {
        "community_members": ["community_members", "community_members_history"],
        "course_lessons_completed": ["course_lesson_completed"],
        "course_completed": ["course_completed"],
        "events": ["events_list", "event_attendees"],
        "member_tags": ["member_tags_list"],
        "post_comments": ["post_comment_created"],
        "post_comment_liked": ["post_comment_liked"],
        "post_liked": ["post_liked"],
    }
        
except Exception as e:
    logger.error(f"❌ Error setting Airbyte configuration: {e}")
    raise e


# Make.com Configuration
try:
    MAKE_WEBHOOK_WEEKLY_REPORTS = os.getenv("MAKE_WEBHOOK_WEEKLY_REPORTS")
    MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION = os.getenv("MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION")
    
    # Legacy single webhook URL (for backward compatibility)
    MAKE_WEBHOOK_URL = MAKE_WEBHOOK_WEEKLY_REPORTS or os.getenv("MAKE_WEBHOOK_URL")
    
    if MAKE_WEBHOOK_WEEKLY_REPORTS is None:
        logger.warning("⚠️ MAKE_WEBHOOK_WEEKLY_REPORTS not set. Weekly email automation will be skipped.")
    else:
        logger.info("✅ Make.com weekly reports webhook configured")
    
    if MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION is None:
        logger.warning("⚠️ MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION not set. Monthly course completion emails will be skipped.")
    else:
        logger.info("✅ Make.com monthly course completion webhook configured")
        
except Exception as e:
    logger.error(f"❌ Error setting Make.com configuration: {e}")
    raise e