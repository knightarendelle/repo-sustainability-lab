# Repo Sustainability Lab

A data-driven exploration of maintenance stagnation risk in open-source GitHub repositories using public event data and interpretable baselines.

This project emphasizes engineering judgment, reproducibility, and honest evaluation over leaderboard performance.

## Motivation

Open-source projects rarely fail suddenly.
They accumulate maintenance debt until progress quietly stalls.

This project asks a narrow, operational question:

**Can we detect early signals of repository stagnation using only public activity data?**

Rather than ranking popularity or code quality, repository health is modeled as a temporal process.

## Key Principles

- Uses only public data (no scraping, no private signals)
- Fully reproducible pipelines
- Explainable features and models
- Cost-aware BigQuery usage with dry-run safeguards

## Data Source

- **GH Archive** (public GitHub event stream)
- Accessed via BigQuery
- Event types include:
  - Push events
  - Pull requests
  - Issues
  - Comments
  - Forks
  - Watch events

No GitHub API rate limits. No scraping.

## Dataset Construction

### Cohort Definition (Dataset v1)

- Public repositories with activity in the last 30 days
- Minimum 50 events in that window
- Uniform random sample of 200 repositories
- Stars and forks are not used as sampling signals to reduce popularity bias

### Feature Engineering

- Weekly aggregation over 26 weeks
- Features include:
  - `events_total`
  - `push_events`
  - `pr_events`
  - `issues_events`
  - comment activity
  - Time-lagged features (t-1, t-2, t-3 weeks)

This produces a compact time-series dataset suitable for modeling decay patterns.

### Label Definition

Stagnation (v1) is defined operationally.

A (repo, week) is labeled `stagnates_60d = 1` if, in the **next 8 weeks**:

- Weekly activity falls below 5 events
- For at least 6 of those 8 weeks

Otherwise, the label is 0.

This creates a forward-looking prediction task, not a descriptive metric.

## Baseline Model

### Model

- **Logistic Regression**
- `class_weight="balanced"`
- Time-safe split (train on earlier weeks, test on later weeks)

### Rationale

- Interpretable coefficients
- Explicit handling of extreme class imbalance
- Establishes a realistic lower bound

### Results (Baseline)

- **Positive rate:** ~0.6%
- **ROC-AUC:** ~0.80
- **PR-AUC (Average Precision):** ~0.017
- **Precision @ top 5% risk:** 0.00

### Interpretation

- The model can rank relative risk (ROC-AUC ≈ 0.8)
- Absolute precision is low due to extreme class imbalance
- True stagnation is a rare event in recently active repositories
- Activity volume alone is a weak early-warning signal

This baseline should be viewed as a **risk flagger**, not a decision system.

## Key Insights

- Sustained low activity across multiple weeks dominates prediction
- Single-week drops are poor predictors
- ROC-AUC can be misleading for rare events
- Precision–recall metrics reveal the true difficulty of the task

## Project Structure

```
.
├── docs/
│   ├── methodology.md
│   └── limitations.md
├── scripts/
│   ├── extract_repo_list.py
│   ├── extract_weekly_features.py
│   ├── build_labels.py
│   └── train_baseline.py
├── data/
│   └── processed/ (generated locally, not committed)
└── README.md
```

## Limitations

- Only public GitHub events are observed
- No semantic understanding of commits or issues
- Forks and archived repos filtered in later versions
- Label thresholds are intentionally conservative

## Future Directions (Optional)

- Survival analysis instead of binary labels
- Contributor concentration metrics
- GPU-accelerated rolling window computations
- Semantic signals from commit messages or issues

## Status

**Phase 1 complete.**

This repository documents an end-to-end workflow:

- data acquisition
- feature engineering
- labeling
- baseline modeling
- honest evaluation

Further work would focus on performance and acceleration, not conceptual correctness.
