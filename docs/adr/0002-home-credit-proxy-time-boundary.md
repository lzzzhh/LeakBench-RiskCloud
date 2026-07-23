# ADR-0002: Home Credit Proxy Time Boundary

**Status:** ACCEPTED
**Date:** 2026-07-23

## Context

The Home Credit Default Risk Kaggle dataset does not provide a reliable,
real-world calendar application date. All temporal fields (`DAYS_CREDIT`,
`DAYS_DECISION`, `MONTHS_BALANCE`, etc.) are expressed as integer offsets
relative to the application moment, without disclosing the anchor date.

## Decision

1. **Prediction point uses a fixed synthetic anchor** (`2000-01-01T00:00:00Z`).
   This is an engineering proxy, not a claim about when loans were issued.

2. **`available_at = prediction_time`** for all application-snapshot features.
   The adapter models availability as "the application snapshot is available
   at the moment of prediction." This does not assert when source systems
   actually received the data.

3. **Label maturity** is a configurable synthetic parameter (default 365 days).
   It does not represent Home Credit's actual default observation window.

4. **OOT split is deterministic proxy holdout**, not calendar-time OOT.
   The split uses a hash of the entity ID, not a real date offset.

5. **`application_test.csv` is unlabeled.** It cannot be used for supervised
   OOT evaluation (AUC, KS, Lift). It remains available for unlabeled batch
   scoring or competition submission only.

6. **`DAYS_CREDIT` and `MONTHS_BALANCE`** express relative history.
   Only non-positive values are admitted (≤ 0). Positive values would
   represent post-prediction information and are rejected.

## Consequences

- If real calendar dates become available in a future data version,
  a new `boundary_version` must be created. V1 semantics must not be
  silently changed.

- All model evaluation metrics computed with this boundary must be
  qualified as "proxy OOT" or "synthetic holdout."

- Upgrading to a real-time boundary requires:
  1. A new `boundary_version` (e.g., `hc-boundary-v2`).
  2. An ADR documenting the real anchor source.
  3. Rebuilding Prediction Points, feature values, and model training
    from the new boundary.
