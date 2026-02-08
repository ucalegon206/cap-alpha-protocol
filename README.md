# Cap Alpha Protocol (CAP)

**Institutional-Grade Intelligence Engine for NFL Capital Velocity & Roster Optimization**

> **Status**: Production (v2.0)
> **Architecture**: Medallion (DuckDB) | Temporal Feature Store
> **Performance**: R² = **0.91** (High-Cap Portfolio Management)

## Overview
The **Cap Alpha Protocol** is a quantitative auditing and predictive inference system designed to price **Second-Order Volatility** in the $20B NFL human capital market. By integrating 15 years of longitudinal contract and play-by-play data, the CAP identifies "Liquidity Traps"—inefficiencies where unforced errors (The Discipline Tax) and structural contract decay silently deplete franchise enterprise value.

Built for **Maximum Fidelity**, the protocol follows two architectural mandates:
1.  **Temporal Integrity**: A strict Date-Based Feature Store (`valid_from`, `valid_until`) ensures zero temporal leakage across 41,000+ observations.
2.  **Portfolio Segmentation**: Statistical focus on high-cap assets (Contracts > $10M) where signal-to-noise is highest (R²=0.91), providing actionable intelligence for franchise decision-makers.

## Key Components

### 1. Medallion Data Architecture (DuckDB)
- **Bronze**: Raw HTML scrapes (Spotrac, PFR) stored as JSON/Parquet.
- **Silver**: Cleaned, typed tables (`dim_players`, `fact_contracts`, `fact_player_efficiency`).
- **Gold**: Feature Store (`feature_values`) and Prediction Results.

### 2. Date-Based Feature Store
A custom-built Feature Store that manages time-travel. Unlike standard "Year-based" models, this system handles precise dates (e.g., September 1st Cutoff).
- **Table**: `feature_values`
- **Columns**: `player_name`, `feature_name`, `feature_value`, `details`, `valid_from` (DATE), `valid_until` (DATE).
- **Retrieval**: `FeatureStore.get_historical_features(as_of_date)`

### 3. XGBoost "Risk Engine"
A Walk-Forward Validation pipeline that trains on past data to predict future inefficiencies.
- **Target**: `edce_risk` (Efficiency Decay)
- **Validation**: Rolling window backtest (2018-2025).
- **Segmentation**: Explicitly segmented by Cap Bucket to isolate "Elite Signal."

## Setup & Usage

### Prerequisites
- macOS (Apple Silicon) / Linux
- Python 3.10+
- **Local Libs Strategy**: Due to environment restrictions, dependencies are installed locally in `./libs`.

### Quickstart
```bash
# 1. Set up environment (Local Libs)
export PYTHONPATH="$(pwd)/libs:$PYTHONPATH"

# 2. Materialize Features (Populate Feature Store)
python3 scripts/materialize_features.py

# 3. Train Model & Generate Predictions
python3 src/train_model.py

# 4. Audit Population Coverage
python3 scripts/population_audit.py
```

## Results Summary (2026 Audit)
The "Low Cap Chaos" hypothesis was confirmed. The model is highly predictive for substantial contracts but random for minimum-wage players.

| Cap Bucket | Count | R2 Score | Strategy |
|------------|-------|----------|----------|
| **High Cap (>$10M)** | 39,734 | **0.91** | **Trust Implicitly** |
| Mid Cap ($2M-$10M) | 114,424 | **0.51** | Use as Signal |
| Low Cap (<$2M) | 332,001 | 0.03 | **Ignore** |
# Dummy
