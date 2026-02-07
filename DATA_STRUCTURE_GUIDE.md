# Data Structure & Schema Guide

This document outlines the core tables in the **Cap Alpha Protocol** Medallion Architecture.

## Schema: Medallion (DuckDB)

### 1. Silver Layer (Cleaned Facts)

#### `fact_player_efficiency`
The central fact table containing player performance and contract data for a given year.
*   **Key**: `player_name`, `year`
*   **Columns**:
    *   `cap_hit_millions`: Annual cap hit (or APY proxy).
    *   `edce_risk`: Efficiency Decay vs. Cap Exposure (Principal Target).
    *   `performance_metrics`: (Various columns like `av`, `epa`, etc.)

### 2. Gold Layer (Feature Store)

#### `feature_values`
The **Date-Based Feature Store**. This table enables Point-in-Time (PIT) correctness by defining strictly when a feature value was "known" to the system.

*   **Key**: `player_name`, `feature_name`, `valid_from`
*   **Columns**:
    *   `player_name` (VARCHAR): Entity identifier.
    *   `prediction_year` (INTEGER): The season the feature applies to (contextual key).
    *   `feature_name` (VARCHAR): Name of the feature (e.g., `lag_1_av`, `interaction_age_cap`).
    *   `feature_value` (DOUBLE): The numeric value.
    *   `details` (VARCHAR): Metadata or JSON blob.
    *   **`valid_from` (DATE)**: The inclusive start date when this value became known.
    *   **`valid_until` (DATE)**: The exclusive end date when this value was superseded (or NULL if current).

**Temporal Logic**:
To retrieve features known as of `2024-09-01`:
```sql
SELECT * FROM feature_values
WHERE valid_from <= '2024-09-01'
  AND (valid_until > '2024-09-01' OR valid_until IS NULL)
```

### 3. Gold Layer (Predictions)

#### `prediction_results`
Stores the output of the `RiskModeler`.
*   **Columns**:
    *   `player_name`
    *   `year`
    *   `predicted_risk_score` (Model Output)
    *   `metadata`: (Team, Position, etc.)

## Data Flow
1.  **Ingestion**: Scrapers $\rightarrow$ JSON $\rightarrow$ `dim_players` / `fact_contracts`.
2.  **Enrichment**: `fact_player_efficiency` is built from Facts.
3.  **Materialization**: `scripts/materialize_features.py` transforms Facts $\rightarrow$ `feature_values` using Date Logic.
4.  **Training**: `FeatureStore.get_historical_features()` pulls a diagonal slice from `feature_values`.
