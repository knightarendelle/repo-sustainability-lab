Treating repository health as a temporal signal, not a static label.

Focus:
- change over time
- relative baselines
- explainable indicators

## Cohort Definition (Dataset v1)

Goal: build a representative sample of active public repositories without using popularity as a proxy.

**Source**
- GH Archive public GitHub event stream (BigQuery)

**Sampling window**
- Repos observed in the last **30 days** (rolling window)

**Inclusion criteria**
- Repository appears in GH Archive events within the sampling window
- Repository has at least **min_events = 50** total events in the sampling window
  (filters out one-off junk repos and reduces noise)

**Exclusion criteria**
- Forks excluded (via GitHub API enrichment step — added in Dataset v2)
- Archived repos excluded (via GitHub API enrichment step — added in Dataset v2)
- Private repos are not present in GH Archive by design

**Sampling method**
- Uniform random sample of **N = 200** repositories from the eligible set

**Notes**
- GH Archive contains only public activity; absence of events ≠ inactive development (e.g., mirrored repos).
- Dataset v1 intentionally avoids stars/forks as sampling signals to reduce popularity bias.
