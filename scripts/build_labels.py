from __future__ import annotations

import os

import pandas as pd


LOW_EVENTS_THRESHOLD = 5          # A)
LOOKAHEAD_WEEKS = 8              # ~60 days
LOW_WEEKS_REQUIRED = 6           # require sustained low activity


def main() -> None:
    in_path = os.path.join("data", "processed", "features_weekly.parquet")
    if not os.path.exists(in_path):
        raise FileNotFoundError(
            f"Missing {in_path}. Run scripts/extract_weekly_features.py first."
        )

    # Read parquet, handling BigQuery date types if present
    try:
        df = pd.read_parquet(in_path)
    except TypeError as e:
        if "dbdate" in str(e) or "not understood" in str(e):
            # Fallback: read with pyarrow directly and convert types
            import pyarrow.parquet as pq
            table = pq.read_table(in_path)
            df = table.to_pandas()
            # Convert any date columns to datetime
            for col in df.columns:
                if df[col].dtype == "object":
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except (ValueError, TypeError):
                        pass
        else:
            raise

    required_cols = {"repo", "week_start", "events_total"}
    missing = required_cols - set(df.columns)
    if missing:
        raise RuntimeError(f"features_weekly.parquet missing columns: {sorted(missing)}")

    # Ensure proper types and ordering
    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df = df.sort_values(["repo", "week_start"]).reset_index(drop=True)

    # Binary low-activity flag for each observed week
    df["low_activity"] = (df["events_total"] < LOW_EVENTS_THRESHOLD).astype("int8")

    # Build label by looking ahead within each repo
    # Use a different approach: iterate over groups and combine results
    results = []
    for repo_name, g in df.groupby("repo", group_keys=False):
        g = g.sort_values("week_start").reset_index(drop=True)

        # For each row i, sum low_activity over rows i+1 ... i+LOOKAHEAD_WEEKS
        # (next 8 weeks). We use shift(-k) to align future weeks.
        future_low_sum = 0
        for k in range(1, LOOKAHEAD_WEEKS + 1):
            future_low_sum += g["low_activity"].shift(-k).fillna(0).astype("int16")

        g["future_low_weeks_8w"] = future_low_sum.astype("int16")
        g["stagnates_60d"] = (g["future_low_weeks_8w"] >= LOW_WEEKS_REQUIRED).astype("int8")

        # Rows near the end don't have full 8-week lookahead -> mark as NaN (unlabeled)
        # Need at least LOOKAHEAD_WEEKS future weeks present.
        g["has_full_lookahead"] = g["low_activity"].shift(-LOOKAHEAD_WEEKS).notna()
        g.loc[~g["has_full_lookahead"], "stagnates_60d"] = pd.NA

        # Ensure 'repo' column is preserved
        g["repo"] = repo_name

        results.append(g)

    labeled = pd.concat(results, ignore_index=True)
    
    # Keep only rows with a valid label
    labeled = labeled[labeled["stagnates_60d"].notna()].copy()
    labeled["stagnates_60d"] = labeled["stagnates_60d"].astype("int8")

    out_dir = os.path.join("data", "processed")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "dataset_labeled.parquet")
    labeled.to_parquet(out_path, index=False)

    print(f"[ok] wrote: {out_path}")
    print(f"[ok] rows labeled: {len(labeled)}")
    print(f"[ok] repos: {labeled['repo'].nunique()}")
    pos_rate = labeled["stagnates_60d"].mean()
    print(f"[ok] positive rate (stagnates_60d=1): {pos_rate:.3f}")
    print(labeled[["repo", "week_start", "events_total", "future_low_weeks_8w", "stagnates_60d"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
