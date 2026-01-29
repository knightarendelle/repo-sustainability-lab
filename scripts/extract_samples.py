from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


@dataclass(frozen=True)
class ExtractConfig:
    gcp_project_id: str
    repo_full_name: str  # e.g. "pallets/flask"
    days: int
    end_date_utc: date  # inclusive (we'll build a BETWEEN)


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def build_query(cfg: ExtractConfig) -> str:
    start = cfg.end_date_utc - timedelta(days=cfg.days - 1)
    start_s = _yyyymmdd(start)
    end_s = _yyyymmdd(cfg.end_date_utc)
    start_suffix = start_s[2:]  # YYMMDD for wildcard 20*
    end_suffix = end_s[2:]

    return f"""
    -- standardSQL
    WITH base AS (
      SELECT
        PARSE_DATE('%Y%m%d', CONCAT('20', _TABLE_SUFFIX)) AS event_date,
        type
      FROM `githubarchive.day.20*`
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\\d{{6}}$')
      AND _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
      AND repo.name = '{cfg.repo_full_name}'
    )
    SELECT
      event_date,
      COUNT(1) AS events_total,
      COUNTIF(type = 'PushEvent') AS push_events,
      COUNTIF(type = 'PullRequestEvent') AS pr_events,
      COUNTIF(type = 'IssuesEvent') AS issues_events,
      COUNTIF(type = 'IssueCommentEvent') AS issue_comment_events,
      COUNTIF(type = 'PullRequestReviewCommentEvent') AS pr_review_comment_events,
      COUNTIF(type = 'WatchEvent') AS watch_events,
      COUNTIF(type = 'ForkEvent') AS fork_events
    FROM base
    GROUP BY event_date
    ORDER BY event_date
    """


def main() -> None:
    load_dotenv()

    gcp_project_id = os.getenv("GCP_PROJECT_ID")
    if not gcp_project_id:
        raise RuntimeError("Missing GCP_PROJECT_ID in .env (copy .env.example -> .env and set it).")

    cfg = ExtractConfig(
        gcp_project_id=gcp_project_id,
        repo_full_name=os.getenv("SEED_REPO", "pallets/flask"),
        days=int(os.getenv("LOOKBACK_DAYS", "7")),
        end_date_utc=date.fromisoformat(os.getenv("END_DATE_UTC", str(date.today()))),
    )

    client = bigquery.Client(project=cfg.gcp_project_id)

    sql = build_query(cfg)

    # Dry run first (prevents accidental big bills)
    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(sql, job_config=dry_config)
    bytes_processed = dry_job.total_bytes_processed
    print(f"[dry-run] bytes to be processed: {bytes_processed:,}")

    # Real query
    job = client.query(sql)
    df = job.result().to_dataframe(create_bqstorage_client=False)

    out_dir = os.path.join("data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "sample_features.parquet")
    df.to_parquet(out_path, index=False)

    print(f"[ok] rows: {len(df)}")
    print(f"[ok] wrote: {out_path}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
