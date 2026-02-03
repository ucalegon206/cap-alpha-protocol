# 🏈 NFL Dead Money: Technical Architecture Guide

**System Version**: 2.0 (Production Hardened)  
**Last Updated**: Feb 2026

## 1. System Overview
The **NFL Dead Money Pipeline** is a hyperscale intelligence system designed to quantify "Cap Toxicity" (fiscal risk) and prescribe strategic actions for NFL front offices. It ingests financial data (Spotrac) and performance metrics (PFR), models the probability of "Dead Money" utilizing XGBoost, and generates automated strategic audits.

### Key Capabilities
- **Risk Frontier Modeling**: Predicts the 24-month probability of a contract becoming "Toxic" (Dead Money > Performance Value).
- **Strategic Engine**: Prescribes specific actions (e.g., "Emergency Purge", "Draft Priority: QB") based on roster risk and depth context.
- **Hyperscale Feature Matrix**: Generates 1000+ features including performance lag, volatility, and salary cap inflation adjustments.

---

## 2. Architecture & Data Flow

### Layer 1: Ingestion (Raw -> Silver)
- **Scrapers**:
    - `src/spotrac_scraper_v2.py`: Fetches Cap Hit, Dead Cap, and Contract Terms. Handles scalar ambiguity.
    - `src/pfr_game_logs.py`: Fetches game-level stats with exponential backoff for 429 rate-limiting.
    - `src/pfr_draft_scraper.py`: Fetches recent draft history (2023-2025) for context.
- **Storage**: Raw CSVs stored in `data/raw/` (Versioned by timestamp).
- **Normalization**: `scripts/ingest_to_duckdb.py` cleans and loads data into **DuckDB** (`silver_` tables).

### Layer 2: Feature Engineering (Silver -> Gold)
- **Feature Factory**: `src/feature_factory.py` transforms raw stats into predictive signals.
    - **Lags**: `total_tds_lag_1`, `total_tds_lag_2` (Stability Metrics).
    - **Volatility**: Rolling standard deviation of performance.
    - **Demographics**: True Age (Birthdate), Experience, Draft Capital.
- **Gold Table**: `fact_player_efficiency` (The "One Big Table" for modeling).

### Layer 3: Predictive Modeling (XGBoost)
- **Target**: `edce_risk` (Expected Dead Cap Exposure).
- **Algorithm**: XGBoost Regressor (Gradient Boosting).
- **Validation**: 5-Fold Time-Series Cross-Validation.
- **Transparency**: SHAP (SHapley Additive exPlanations) values generated for every prediction.

### Layer 4: Strategic Intelligence (The "Brain")
- **Engine**: `src/strategic_engine.py` (Centralized Logic).
- **Prescription Logic**:
    1. **Risk Assessment**: Classifies teams as "Emergency Purge", "Structural Rebalancing", etc.
    2. **Context Check**:
        - **FA Check**: Did they sign a "Big Splash" FA (>$10M APY) in 2025? -> `FA Solution`
        - **Draft Check**: Did they draft a successor (Rd 1-3) in 2023-2025? -> `Develop Successor`
    3. **Recommendation**: If no solution found -> `Draft Priority: [Position]`.

---

## 3. Operational Playbook

### Running the Pipeline
The entire system is orchestrated via `run_pipeline.py`, which mimics a production Airflow DAG.

```bash
# Activate Virtual Env
source .venv/bin/activate

# Execute End-to-End Pipeline
python run_pipeline.py
```

### Testing & Integrity
The system includes a formal **Pytest** suite to ensure logic correctness and data integrity.

```bash
# Run Unit & Integrity Tests
python -m pytest tests/
```
- **Integrity**: Verifies 32 teams exist, no NULL critical fields, and successful 2025/2026 data sync.
- **Logic**: Verifies "Successor Suppression" and "FA Splash" logic using mocks.

---

## 4. Directory Structure
- `src/`: Core libraries (`StrategicEngine`, `SpotracScraper`, `FeatureFactory`).
- `scripts/`: ETL and Reporting scripts (`ingest_to_duckdb.py`, `generate_team_prescriptions.py`).
- `tests/`: Pytest suite (`test_strategic_engine.py`, `test_data_integrity.py`).
- `data/`: DuckDB instance (`nfl_data.db`) and raw CSVs.
- `reports/`: Markdown audits and visualizations.

## 5. Further Documentation
- [Data Structure Guide (v2.0)](DATA_STRUCTURE_GUIDE.md) - Deep dive into Bronze/Silver/Gold DuckDB Schema.
- [Pipeline Architecture](PIPELINE_DOCUMENTATION.md) - End-to-end orchestration flow (`run_pipeline.py`).
- [Testing & Integrity](TESTING.md) - Guide to `pytest` suites and Triple Check gates.
