from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
)
from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True)
class TrainConfig:
    labeled_path: str = "data/processed/dataset_labeled.parquet"
    target: str = "stagnates_60d"
    lags: int = 3
    test_frac: float = 0.2  # last 20% of time (global) for test
    min_rows_per_repo: int = 8  # after lagging
    random_state: int = 42


BASE_FEATURES = [
    "events_total",
    "push_events",
    "pr_events",
    "issues_events",
    "issue_comment_events",
    "pr_review_comment_events",
    "watch_events",
    "fork_events",
]


def make_lag_features(df: pd.DataFrame, base_cols: List[str], lags: int) -> pd.DataFrame:
    df = df.sort_values(["repo", "week_start"]).copy()
    for k in range(1, lags + 1):
        for c in base_cols:
            if c in df.columns:
                df[f"{c}_lag{k}"] = df.groupby("repo")[c].shift(k)
    return df


def time_split(df: pd.DataFrame, test_frac: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # Global time-based split to avoid lookahead leakage
    df = df.sort_values("week_start").copy()
    cutoff_idx = int(len(df) * (1 - test_frac))
    cutoff_time = df.iloc[cutoff_idx]["week_start"]
    train = df[df["week_start"] < cutoff_time].copy()
    test = df[df["week_start"] >= cutoff_time].copy()
    return train, test


def print_top_risk(test_df: pd.DataFrame, proba: np.ndarray, k: int = 25) -> None:
    tmp = test_df[["repo", "week_start", "events_total", "stagnates_60d"]].copy()
    tmp["risk"] = proba
    tmp = tmp.sort_values("risk", ascending=False).head(k)
    hits = int(tmp["stagnates_60d"].sum())
    print(f"\n[top {k}] predicted risk rows (hits={hits}/{k})")
    print(tmp.to_string(index=False))


def main() -> None:
    cfg = TrainConfig()

    if not os.path.exists(cfg.labeled_path):
        raise FileNotFoundError(
            f"Missing {cfg.labeled_path}. Run scripts/build_labels.py first."
        )

    df = pd.read_parquet(cfg.labeled_path)

    required = {"repo", "week_start", cfg.target}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {sorted(missing)}")

    df = df.copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df[cfg.target] = df[cfg.target].astype(int)

    # Ensure only features that exist
    base_cols = [c for c in BASE_FEATURES if c in df.columns]
    if "events_total" not in base_cols:
        raise RuntimeError("events_total must be present in labeled dataset.")

    # Create lag features
    df = make_lag_features(df, base_cols, cfg.lags)

    # Drop rows with missing lag features (early weeks per repo)
    feature_cols = []
    for c in base_cols:
        feature_cols.append(c)
        for k in range(1, cfg.lags + 1):
            feature_cols.append(f"{c}_lag{k}")

    # Keep only columns that actually exist (in case some event types weren't extracted)
    feature_cols = [c for c in feature_cols if c in df.columns]

    df_model = df.dropna(subset=feature_cols).copy()

    # Filter repos with too few rows after lagging (stabilizes training)
    counts = df_model.groupby("repo").size()
    keep_repos = counts[counts >= cfg.min_rows_per_repo].index
    df_model = df_model[df_model["repo"].isin(keep_repos)].copy()

    if len(df_model) < 200:
        print("[warn] Very small dataset after filtering. Consider reducing min_rows_per_repo.")
    pos_rate = df_model[cfg.target].mean()
    print(f"[data] rows={len(df_model)} repos={df_model['repo'].nunique()} pos_rate={pos_rate:.4f}")

    # Split by time
    train_df, test_df = time_split(df_model, cfg.test_frac)
    print(f"[split] train_rows={len(train_df)} test_rows={len(test_df)}")
    print(f"[split] train_pos_rate={train_df[cfg.target].mean():.4f} test_pos_rate={test_df[cfg.target].mean():.4f}")

    X_train = train_df[feature_cols].to_numpy()
    y_train = train_df[cfg.target].to_numpy()
    X_test = test_df[feature_cols].to_numpy()
    y_test = test_df[cfg.target].to_numpy()

    # Scale for logistic regression
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    # Imbalance-aware logistic regression
    clf = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        solver="lbfgs",
    )
    clf.fit(X_train_s, y_train)

    proba = clf.predict_proba(X_test_s)[:, 1]

    # Metrics
    roc = roc_auc_score(y_test, proba) if len(np.unique(y_test)) > 1 else float("nan")
    ap = average_precision_score(y_test, proba) if len(np.unique(y_test)) > 1 else float("nan")
    print(f"\n[metrics] ROC-AUC: {roc:.4f}")
    print(f"[metrics] PR-AUC (Average Precision): {ap:.4f}")

    # Precision/recall at top-risk slice
    k = max(10, int(0.05 * len(test_df)))  # top 5%
    top_idx = np.argsort(-proba)[:k]
    top_precision = y_test[top_idx].mean() if k > 0 else float("nan")
    print(f"[metrics] Precision@top5% (k={k}): {top_precision:.4f}")

    # Optional: print top risky predictions for inspection
    print_top_risk(test_df, proba, k=min(25, len(test_df)))

    # Coefficients (interpretability)
    coef = pd.DataFrame({"feature": feature_cols, "coef": clf.coef_[0]})
    coef["abs_coef"] = coef["coef"].abs()
    coef = coef.sort_values("abs_coef", ascending=False).head(15)
    print("\n[top coefficients]")
    print(coef[["feature", "coef"]].to_string(index=False))


if __name__ == "__main__":
    main()
