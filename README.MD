# RealPars Community Analytics Pipeline

An automated data pipeline that extracts community engagement data from Circle.so, transforms it through analytics layers, and delivers personalized weekly and monthly reports to team managers via email.

## Architecture Overview

```
Circle.so API → Airbyte Cloud → BigQuery (Raw) → dbt (Transform) → BigQuery (Analytics) → Make.com → Email Reports
```

The pipeline is orchestrated by **Dagster Cloud** and follows a modular ELT pattern with clear separation between extraction, transformation, and delivery.

---

## Pipeline Flow

### 1. Data Extraction (Airbyte)

Airbyte Cloud syncs six data streams from the Circle.so API into BigQuery raw tables:

| Source | Data |
|--------|------|
| Community Members | Member profiles and history |
| Course Completions | Course lesson completion records |
| Events & Attendees | Live class events and attendance |
| Post Comments | Community post comments |
| Comment Likes | Likes on comments |
| Post Likes | Likes on posts |

Each connector authenticates via Circle.so Bearer tokens with pagination (100 records/page) and retry logic for rate limits.

### 2. Data Transformation (dbt)

dbt transforms raw data through three layers, each in its own BigQuery schema:

```
Raw Tables
  │
  ▼
Staging (views) ─── cc_stg_clean
  │  Clean & standardize raw data: parse names, emails, JSON member tags
  │
  ▼
Intermediate (incremental) ─── cc_intermediate_transformations
  │  Aggregate weekly engagement metrics per member:
  │  • Live classes attended
  │  • Lessons completed
  │  • Likes received
  │  • Comments made & received
  │  → Combined into a master weekly engagement table
  │
  ▼
Marts (tables) ─── cc_make_com_weekly_reports / cc_sty_weekly_reports
     Business-ready reporting views:
     • Manager-team engagement reports
     • Weighted point rankings
     • Top performers & top movers
     • Make.com helper views for email generation
```

**Engagement Scoring** — Activities are scored with a weighted point system:

| Activity | Points |
|----------|--------|
| Live class attended | 5 |
| Lesson completed | 1 |
| Like received | 1 |
| Comment received | 1 |
| Comment made | 1 |

Live classes are weighted highest to reflect their engagement importance. The system also identifies **Top Movers** by comparing week-over-week point changes.

### 3. Report Delivery (Make.com)

After dbt completes, Dagster triggers a Make.com webhook. Make.com then:

1. Queries BigQuery helper views for each manager's team data
2. Generates a PDF with detailed team engagement breakdowns
3. Populates an HTML email template with rankings, top performers, top movers, and action items
4. Sends a personalized email to each manager

---

## Dagster Orchestration

### Assets

| Asset | Group | Description |
|-------|-------|-------------|
| Airbyte sync assets (dynamic) | `airbyte_to_bigquery_sync` | One asset per Airbyte connection |
| `realpars_dbt_models` | `dbt_transformations` | Runs `dbt build` (depends on all Airbyte assets) |
| `trigger_make_weekly_reports` | `email_automation` | Fires Make.com webhook for weekly emails |
| `trigger_make_monthly_course_completion` | `email_automation` | Fires Make.com webhook for monthly emails |

### Asset Dependency Graph

```
Airbyte Connection 1 ─┐
Airbyte Connection 2 ─┤
Airbyte Connection 3 ─┼──▶ realpars_dbt_models ──┬──▶ trigger_make_weekly_reports
Airbyte Connection 4 ─┤                          └──▶ trigger_make_monthly_course_completion
Airbyte Connection 5 ─┤
Airbyte Connection 6 ─┘
```

### Jobs

| Job | What It Runs |
|-----|-------------|
| `weekly_report_pipeline` | Full weekly cycle: Airbyte → dbt → weekly email trigger |
| `monthly_course_completion_pipeline` | Full monthly cycle: Airbyte → dbt → monthly email trigger |
| `airbyte_and_dbt_refresh` | Data refresh without triggering emails |
| `airbyte_sync_only` | Raw data extraction only |
| `dbt_transform_only` | Transformation only (assumes fresh data) |
| `email_trigger_weekly_only` | Fire weekly webhook only |
| `email_trigger_monthly_only` | Fire monthly webhook only |

### Schedules

| Schedule | Cron | Runs | Frequency |
|----------|------|------|-----------|
| `weekly_report_schedule` | `0 7 * * 1` (Mon 8 AM CET) | `weekly_report_pipeline` | Weekly |
| `monthly_course_completion_schedule` | `0 8 1 * *` (1st of month, 9 AM CET) | `monthly_course_completion_pipeline` | Monthly |
| `weekly_data_refresh` | `0 23 * * 0` (Mon midnight CET) | `airbyte_sync_only` | Weekly |

Cron expressions are in UTC; the pipeline is designed for EU (CET) business hours.

---

## dbt Model Dependency Graph

```
Sources (BigQuery Raw — cc_raw_*)
└── Staging (cc_stg_clean)
    ├── clean_communtity_members_table
    ├── clean_events_attendees_table
    ├── clean_course_lesson_completed_table
    ├── clean_post_liked_table
    ├── clean_post_comments_table
    └── clean_post_comment_liked_table
        │
        ▼
Intermediate (cc_intermediate_transformations)
    ├── int_weekly_live_classes_attended
    ├── int_weekly_lessons_completed
    ├── int_weekly_likes_received
    ├── int_weekly_comments_received
    └── int_weekly_comments_made
        └──────────────────────────────────────────────────┐
                                                           ▼
                                          int_weekly_member_engagement_incremental
                                                           │
Business Relationships (cc_stg_business_relationships)     │
    └── business_relationships ────────────────────────┐   │
                                                       ▼   ▼
Marts (cc_stg_weekly_reports)              weekly_team_engagement_report
                                                        │
          ┌─────────────────────────────────────────────┼──────────────────────────┐
          ▼                                             ▼                          ▼
manager_weekly_team_detail_report  manager_weekly_report_aggregated  manager_email_report_with_rankings
          │                                             │                          │
          └─────────────────────────────────────────────┴──────────────────────────┘
                                                        ▼
                                    Make.com Export Models (cc_make_com_weekly_reports)
                                        ├── manager_email_list_makecom
                                        ├── manager_email_top_performers_makecom
                                        ├── manager_email_top_movers_makecom
                                        ├── manager_email_action_points_makecom
                                        ├── manager_team_detail_pdf_makecom
                                        ├── manager_pdf_movers_actions_makecom
                                        ├── summary_pdf
                                        └── report_delivery_log
```

`weekly_team_engagement_report` is the central hub — all downstream models read from it. It filters to the **last completed week** (`week_start_date < DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))`) so reports always reflect the previous Monday–Sunday period.

---

## Project Structure

```
├── dagster_project/          # Dagster orchestration
│   ├── definitions.py        # Jobs, schedules, resources, asset definitions
│   ├── assets/               # Airbyte, dbt, and Make.com trigger assets
│   ├── jobs/                 # Job definitions
│   └── resources/            # Airbyte Cloud and dbt CLI resources
├── dbt/realpars_community/   # dbt project
│   ├── models/
│   │   ├── staging/          # Raw data cleaning (views)
│   │   ├── intermediate/     # Weekly metric aggregation (incremental)
│   │   └── marts/            # Reporting tables (partitioned & clustered)
│   └── macros/               # Custom schema naming
├── airbyte_connectors/       # Airbyte connection configurations (YAML)
├── configs/                  # Environment variable parsing and config
├── data/                     # Reference CSVs (business/manager mappings)
├── make.com/                 # Email HTML templates
├── secrets/                  # Credentials (gitignored)
├── terraform/                # Infrastructure config
├── dagster_cloud.yaml        # Dagster Cloud deployment
└── requirements.txt          # Python dependencies
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Orchestration | Dagster Cloud |
| Extraction | Airbyte Cloud |
| Warehouse | Google BigQuery |
| Transformation | dbt (dbt-bigquery) |
| Email Automation | Make.com |
| Cloud Platform | Google Cloud Platform |
| Languages | Python 3.10+, SQL |

---

## Environment Configuration

The pipeline requires environment variables for each integration:

- **Airbyte** — `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_CONNECTION_IDS`
- **Google Cloud** — `GOOGLE_APPLICATION_CREDENTIALS`, `PROJECT_ID`, `ACTIVE_ENVIRONMENT` (`prod`/`dev`)
- **BigQuery** — Dataset names for raw, staging, intermediate, and mart schemas
- **Make.com** — `MAKE_WEBHOOK_WEEKLY_REPORTS`, `MAKE_WEBHOOK_MONTHLY_COURSE_COMPLETION`
- **Circle.so** — API tokens for data and admin APIs

See [`dagster_project/QUICK_START.md`](dagster_project/QUICK_START.md) for setup instructions.