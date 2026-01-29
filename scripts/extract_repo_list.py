from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, timedelta

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


@dataclass(frozen=True)
class RepoListConfig:
    gcp_project_id: str
    days: int = 30
    n_repos: int = 200
    min_events: int = 50
    end_date_utc: date = date.today()  # inclusive


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def build_query(cfg: RepoListConfig) -> str:
    start = cfg.end_date_utc - timedelta(days=cfg.days - 1)
    start_s = _yyyymmdd(start)
    end_s = _yyyymmdd(cfg.end_date_utc)
    start_suffix = start_s[2:]  # YYMMDD
    end_suffix = end_s[2:]      # YYMMDD

    # NOTE: Use a more specific prefix (20*) so wildcard never matches views like "yesterday".
    # _TABLE_SUFFIX here is YYMMDD (6 digits), we rebuild full date with CONCAT('20', _TABLE_SUFFIX) if needed.
    return f"""
    --standardSQL
    WITH recent AS (
      SELECT
        repo.name AS repo_name
      FROM `githubarchive.day.20*`
      WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\\d{{6}}$')
        AND _TABLE_SUFFIX BETWEEN '{start_suffix}' AND '{end_suffix}'
        AND repo.name IS NOT NULL
    ),
    eligible AS (
      SELECT
        repo_name,
        COUNT(1) AS events_30d
      FROM recent
      GROUP BY repo_name
      HAVING events_30d >= {cfg.min_events}
    )
    SELECT repo_name, events_30d
    FROM eligible
    ORDER BY RAND()
    LIMIT {cfg.n_repos}
    """


def main() -> None:
    load_dotenv()
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("Missing GCP_PROJECT_ID in .env")

    cfg = RepoListConfig(
        gcp_project_id=project_id,
        days=int(os.getenv("COHORT_DAYS", "30")),
        n_repos=int(os.getenv("N_REPOS", "200")),
        min_events=int(os.getenv("MIN_EVENTS", "50")),
        end_date_utc=date.fromisoformat(os.getenv("END_DATE_UTC", str(date.today()))),
    )

    client = bigquery.Client(project=cfg.gcp_project_id)
    sql = build_query(cfg)

    # Dry-run guardrail
    dry = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(sql, job_config=dry)
    print(f"[dry-run] bytes to be processed: {dry_job.total_bytes_processed:,}")

    df = client.query(sql).result().to_dataframe(create_bqstorage_client=False)

    out_dir = os.path.join("data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "repo_list.csv")
    df.to_csv(out_path, index=False)

    print(f"[ok] repos: {len(df)}")
    print(f"[ok] wrote: {out_path}")
    print(df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
