# scripts/plot_results.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.preprocessing import StandardScaler


@dataclass(frozen=True)
class PlotConfig:
    labeled_path: str = "data/processed/dataset_labeled.parquet"
    target: str = "stagnates_60d"
    lags: int = 3
    test_frac: float = 0.2  # last 20% of time for test (time-safe)
    min_rows_per_repo: int = 8  # after lagging
    out_dir: str = "artifacts"
    pr_zoom_max: float = 0.05  # y-axis max for zoomed PR plot
    pr_use_log_y: bool = False  # set True for log-scale precision axis


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


def ensure_out_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def make_lag_features(df: pd.DataFrame, base_cols: List[str], lags: int) -> pd.DataFrame:
    df = df.sort_values(["repo", "week_start"]).copy()
    for k in range(1, lags + 1):
        for c in base_cols:
            if c in df.columns:
                df[f"{c}_lag{k}"] = df.groupby("repo")[c].shift(k)
    return df


def time_split(df: pd.DataFrame, test_frac: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("week_start").copy()
    cutoff_idx = int(len(df) * (1 - test_frac))
    cutoff_time = df.iloc[cutoff_idx]["week_start"]
    train = df[df["week_start"] < cutoff_time].copy()
    test = df[df["week_start"] >= cutoff_time].copy()
    return train, test


def set_bold_style() -> None:
    plt.rcParams.update({
        "figure.dpi": 160,
        "savefig.dpi": 160,
        "font.size": 14,
        "axes.titlesize": 20,
        "axes.labelsize": 16,
        "xtick.labelsize": 13,
        "ytick.labelsize": 13,
        "legend.fontsize": 13,
        "axes.titleweight": "bold",
        "axes.labelweight": "bold",
        "lines.linewidth": 3,
    })


def save_fig(path: str) -> None:
    plt.tight_layout()
    plt.savefig(path, bbox_inches="tight")
    plt.close()

def plot_class_imbalance(df_model: pd.DataFrame, target: str, out_dir: str) -> None:
    pos = int(df_model[target].sum())
    total = int(len(df_model))
    neg = total - pos

    pos_pct = 100.0 * pos / max(1, total)
    neg_pct = 100.0 * neg / max(1, total)

    plt.figure(figsize=(10, 4))

    # 100% stacked horizontal bar
    plt.barh(["All Samples"], [neg_pct], height=0.6)
    plt.barh(["All Samples"], [pos_pct], left=[neg_pct], height=0.6)

    plt.title("Class Imbalance (Model-ready dataset)")
    plt.xlabel("Percent of samples")
    plt.xlim(0, 100)

    # Main percentage labels
    plt.text(neg_pct / 2, 0, f"{neg_pct:.2f}%", ha="center", va="center", fontweight="bold")

    if pos_pct >= 1:
        plt.text(neg_pct + pos_pct / 2, 0, f"{pos_pct:.2f}%", ha="center", va="center", fontweight="bold")
    else:
        # If too tiny, float label above
        plt.text(neg_pct + pos_pct, 0.25, f"{pos_pct:.2f}%", ha="right", va="bottom", fontweight="bold")

    # Odds stat (this is the punch)
    if pos > 0:
        odds = int(round(total / pos))
        odds_text = f"≈ 1 positive per {odds} samples"
    else:
        odds_text = "No positive samples"

    plt.figtext(0.01, 0.12, odds_text, ha="left", fontsize=11, fontweight="bold")
    plt.figtext(0.01, 0.05, f"n={total}  positives={pos}  negatives={neg}", ha="left", fontsize=11)

    save_fig(os.path.join(out_dir, "01_class_imbalance.png"))


def plot_activity_decay(df_model: pd.DataFrame, target: str, out_dir: str) -> None:
    # Mean events_total at t, t-1, t-2, t-3 for positives vs negatives
    cols = ["events_total", "events_total_lag1", "events_total_lag2", "events_total_lag3"]
    cols = [c for c in cols if c in df_model.columns]

    pos = df_model[df_model[target] == 1]
    neg = df_model[df_model[target] == 0]

    pos_means = [pos[c].mean() if len(pos) else np.nan for c in cols]
    neg_means = [neg[c].mean() if len(neg) else np.nan for c in cols]

    x_labels = []
    for c in cols:
        if c == "events_total":
            x_labels.append("t")
        else:
            k = c.split("lag")[-1]
            x_labels.append(f"t-{k}")

    plt.figure(figsize=(9, 5))
    plt.plot(x_labels, neg_means, marker="o", label="Non-stagnation (0)")
    plt.plot(x_labels, pos_means, marker="o", label="Stagnation (1)")
    plt.title("Activity Trend (Weekly events_total)")
    plt.ylabel("Mean weekly events_total")
    plt.xlabel("Weeks (relative to prediction time)")
    plt.legend()

    plt.figtext(
        0.01, 0.01,
        "Note: positives are rare; trend is illustrative (means over available samples).",
        ha="left", fontsize=11
    )
    save_fig(os.path.join(out_dir, "02_activity_decay.png"))


def train_and_get_scores(df_model: pd.DataFrame, target: str, feature_cols: List[str], test_frac: float):
    train_df, test_df = time_split(df_model, test_frac)

    X_train = train_df[feature_cols].to_numpy()
    y_train = train_df[target].to_numpy()
    X_test = test_df[feature_cols].to_numpy()
    y_test = test_df[target].to_numpy()

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    clf = LogisticRegression(max_iter=2000, class_weight="balanced", solver="lbfgs")
    clf.fit(X_train_s, y_train)
    scores = clf.predict_proba(X_test_s)[:, 1]
    return y_test, scores


def plot_precision_recall(
    df_model: pd.DataFrame,
    target: str,
    feature_cols: List[str],
    test_frac: float,
    out_dir: str,
    pr_zoom_max: float,
    pr_use_log_y: bool,
) -> None:
    y_test, scores = train_and_get_scores(df_model, target, feature_cols, test_frac)

    if y_test.sum() == 0:
        plt.figure(figsize=(8, 5))
        plt.title("Precision–Recall Curve")
        plt.text(
            0.5, 0.5,
            "No positive samples in test set.\nTry a larger cohort or different split.",
            ha="center", va="center", fontweight="bold"
        )
        plt.axis("off")
        save_fig(os.path.join(out_dir, "03_precision_recall.png"))
        return

    precision, recall, _ = precision_recall_curve(y_test, scores)
    ap = average_precision_score(y_test, scores)
    roc = roc_auc_score(y_test, scores) if len(np.unique(y_test)) > 1 else float("nan")
    base_rate = float(y_test.mean())

    plt.figure(figsize=(8, 5))
    plt.plot(recall, precision, label="Model")
    plt.hlines(base_rate, 0, 1, linestyles="dashed", linewidth=2, label="Baseline (base rate)")

    # Zoom or log-scale for rare-event PR curves
    if pr_use_log_y:
        plt.yscale("log")
        # Keep the lower bound above 0 for log scale
        plt.ylim(max(base_rate / 2, 1e-6), 0.2)
    else:
        ymax = max(float(np.nanmax(precision)) * 1.2, base_rate * 3)
        plt.ylim(0, min(ymax, pr_zoom_max))

    plt.xlim(0, 1)
    plt.title("Precision–Recall Curve (Time-safe test split, zoomed)")
    plt.xlabel("Recall")
    plt.ylabel("Precision")
    plt.legend()

    plt.figtext(
        0.01, 0.01,
        f"PR-AUC={ap:.4f}   ROC-AUC={roc:.4f}   Base rate={base_rate:.4f}",
        ha="left", fontsize=11
    )
    save_fig(os.path.join(out_dir, "03_precision_recall.png"))


def main() -> None:
    cfg = PlotConfig()

    if not os.path.exists(cfg.labeled_path):
        raise FileNotFoundError(f"Missing {cfg.labeled_path}. Run scripts/build_labels.py first.")

    set_bold_style()
    ensure_out_dir(cfg.out_dir)

    df = pd.read_parquet(cfg.labeled_path).copy()
    df["week_start"] = pd.to_datetime(df["week_start"])
    df[cfg.target] = df[cfg.target].astype(int)

    base_cols = [c for c in BASE_FEATURES if c in df.columns]
    if "events_total" not in base_cols:
        raise RuntimeError("events_total not found in labeled dataset")

    df = make_lag_features(df, base_cols, cfg.lags)

    # Build feature list: current + lags
    feature_cols: List[str] = []
    for c in base_cols:
        feature_cols.append(c)
        for k in range(1, cfg.lags + 1):
            col = f"{c}_lag{k}"
            if col in df.columns:
                feature_cols.append(col)

    # Model-ready rows: drop NaNs created by lagging
    df_model = df.dropna(subset=feature_cols).copy()

    # Filter repos with enough rows after lagging
    counts = df_model.groupby("repo").size()
    keep = counts[counts >= cfg.min_rows_per_repo].index
    df_model = df_model[df_model["repo"].isin(keep)].copy()

    print(
        f"[data] model-ready rows={len(df_model)} repos={df_model['repo'].nunique()} "
        f"pos_rate={df_model[cfg.target].mean():.4f}"
    )

    plot_class_imbalance(df_model, cfg.target, cfg.out_dir)
    plot_activity_decay(df_model, cfg.target, cfg.out_dir)
    plot_precision_recall(
        df_model, cfg.target, feature_cols, cfg.test_frac, cfg.out_dir, cfg.pr_zoom_max, cfg.pr_use_log_y
    )

    print(f"[ok] wrote plots to: {cfg.out_dir}/")
    print(" - 01_class_imbalance.png")
    print(" - 02_activity_decay.png")
    print(" - 03_precision_recall.png")
    if cfg.pr_use_log_y:
        print("PR curve uses log-scale y-axis.")
    else:
        print(f"PR curve zoomed to y <= {cfg.pr_zoom_max}.")


if __name__ == "__main__":
    main()
