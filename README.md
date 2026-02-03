# Repo Sustainability Lab

A data-driven system for estimating **maintenance debt and stagnation risk**
in open-source GitHub repositories using public event data.

## What this is
This project models repository health over time, focusing on:
- activity decay
- responsiveness
- contributor concentration

It does **not** rank popularity or code quality.

## Data
Uses public GitHub event data (GH Archive via BigQuery).
No private data is collected.

Baseline Results (v1)

A logistic regression baseline using weekly activity features achieved a ROC-AUC of ~0.80 but very low precision at high-risk thresholds due to extreme class imbalance (~0.6% positives).

This suggests that early stagnation detection in active repositories is inherently difficult using activity volume alone, and that such models are better suited as weak risk indicators rather than decision systems.