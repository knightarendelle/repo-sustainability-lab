from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


WEEKS = 26


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def main() -> None:
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("Missing GCP_PROJECT_ID in .env")

    repo_list_path = "data/processed/repo_list.csv"
    if not os.path.exists(repo_list_path):
        raise FileNotFoundError("repo_list.csv not found. Run extract_repo_list.py first.")

    repos = (
        pd.read_csv(repo_list_path)["repo_name"]
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    end_date = date.today()
    start_date = end_date - timedelta(days=WEEKS * 7 - 1)

    start_s = _yyyymmdd(start_date)
    end_s = _yyyymmdd(end_date)
    start_suffix = start_s[2:]  # YYMMDD for wildcard 20*
    end_suffix = end_s[2:]

    # Build repo filter for IN clause
    repo_filter = "', '".join(repos)
    
    sql = f"""
    --standardSQL
    WITH base AS (
      SELECT
        repo.name AS repo,
        PARSE_DATE('%Y%m%d', CONCAT('20', _TABLE_SUFFIX)) AS event_date
      FROM `githubarchive.day.20*`
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\\d{{6}}$')
        AND _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        AND repo.name IN ('{repo_filter}')
    ),
    weekly AS (
      SELECT
        repo,
        DATE_TRUNC(event_date, WEEK) AS week_start,
        COUNT(1) AS events_total
      FROM base
      GROUP BY repo, week_start
    )
    SELECT repo, week_start, events_total
    FROM weekly
    ORDER BY repo, week_start
    """

    client = bigquery.Client(project=project_id)

    # Dry run first (prevents accidental big bills)
    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(sql, job_config=dry_config)
    bytes_processed = dry_job.total_bytes_processed
    print(f"[dry-run] bytes to be processed: {bytes_processed:,}")

    # Real query
    job = client.query(sql)
    df = job.result().to_dataframe(create_bqstorage_client=False)

    # Convert BigQuery date types to standard pandas datetime for parquet compatibility
    df["week_start"] = pd.to_datetime(df["week_start"])

    out_dir = os.path.join("data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "features_weekly.parquet")
    df.to_parquet(out_path, index=False)

    print(f"[ok] rows: {len(df)}")
    print(f"[ok] repos: {df['repo'].nunique()}")
    print(f"[ok] wrote: {out_path}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
