# Data Enrichment Implementation - Dead Money Prediction

## Summary

Implemented data enrichment for NFL Dead Money analysis to enable predictive modeling of players likely to become dead money. The implementation adds critical contract financial data and player performance features required for prediction.

---

## What Was Added

### 1. **Spotrac Contract Scraper** (`src/spotrac_scraper_v2.py`)

**New Method**: `scrape_player_contracts(year: int)`
- Iterates through all 32 NFL team contract pages on Spotrac
- Extracts contract details: guaranteed money, signing bonus, contract length, years remaining
- Rate-limited with 1-second delays between teams
- Quality checks: ≥500 records, ≥25 teams, reasonable value ranges

**Supporting Functions**:
- `scrape_and_save_player_contracts()` - CLI wrapper to save timestamped CSV
- `_normalize_player_contract_df()` - Standardizes column names and parses monetary values
- `_validate_player_contract_data()` - Quality gates (nulls, ranges, team coverage)

**CLI Command**:
```bash
python src/spotrac_scraper_v2.py player-contracts 2024
```

---

### 2. **Enhanced Normalization** (`src/normalization.py`)

**New Functions**:
- `normalize_player_contracts()` - Joins contract data with PFR roster data (age, performance metrics)
- `normalize_dead_money_with_features()` - Combines dead money records with contract + roster features for ML training

**Data Enrichment**:
- Joins on `(player_name, team, year)` to add:
  - Player age at signing (from PFR)
  - Performance metrics (Games, Games Started, Approximate Value)
  - Years of NFL experience
  - Guaranteed money percentage
  - Contract structure characteristics

**Output**: Two enriched CSV files per year:
- `stg_player_contracts_{year}.csv` - Normalized contracts with roster data
- `dead_money_features_{year}.csv` - Dead money with prediction features

---

### 3. **dbt Staging Model** (`dbt/models/staging/stg_player_contracts.sql`)

Standardizes and validates contract data with:
- Column name normalization
- Type casting (decimals for money, integers for years)
- Derived features: `guaranteed_pct` (guaranteed / total value)
- Quality filters: non-null contracts with financial data

---

### 4. **dbt Rosters Model** (`dbt/models/staging/stg_player_rosters.sql`)

Converts PFR raw roster data to structured format:
- Age, games played, games started, performance metrics
- College and draft information
- Quality filters: valid ages (0-50), non-negative values

---

### 5. **dbt Prediction Features Mart** (`dbt/models/marts/fct_player_dead_money_features.sql`)

**ML-Ready Feature Table** joining three data sources:
```
Contracts + Rosters + Dead Money → Prediction Features
```

**Contract Features**:
- `total_contract_value_millions`
- `guaranteed_money_millions`
- `signing_bonus_millions`
- `contract_length_years`
- `years_remaining`
- `guaranteed_pct` (derived)

**Roster Features**:
- `age_at_signing`
- `games_played_prior_year`
- `performance_av` (Approximate Value)
- `years_experience`

**Categorical Features** (derived for modeling):
- `guarantee_category` (high/moderate/low)
- `contract_length_category` (long/medium/short-term)
- `age_category` (veteran/prime/young)
- `performance_category` (elite/good/average/below_average)

**Target Variable**:
- `became_dead_money_next_year` (binary flag)
- `dead_money_amount` (continuous)

---

### 6. **Ingestion Updates** (`src/ingestion.py`)

**New Function**: `stage_player_contracts()`
- Loads raw contract CSVs from `data/raw/`
- Normalizes columns and types
- Outputs to `data/staging/`

**CLI Update**: Added `spotrac-contracts` source option
```bash
python src/ingestion.py --source spotrac-contracts --year 2024
```

---

### 7. **Airflow DAG Updates** (`dags/nfl_dead_money_pipeline.py`)

**New Layer 1 Task**: `scrape_spotrac_player_contracts`
- Parallel execution with other Layer 1 scrapers
- Rate-limited via `external_api` pool (1 slot)

**New Layer 2 Task**: `stage_spotrac_contracts`
- Stages raw contract data to staging layer

**Updated Dependencies**:
```
Layer 1: Scrapers (including player-contracts) [parallel]
    ↓
Layer 2: Staging tasks [parallel within layer]
    ↓
Layer 3: Normalization (now includes contracts + features)
    ↓
Layer 4: dbt (seed → staging → marts)
    ↓
Layer 5: Validation
    ↓
Layer 6: Notebooks
```

---

## Data Flow

```
┌─ Team Caps ─────┐
│                 ├─ STAGING ─ NORMALIZATION ┐
├─ Player Rankings┤                          ├─ dbt SEED ─ dbt RUN
│                 ├─ STAGING ─ NORMALIZATION ┤    ↓         ↓
├─ PFR Rosters ──┤          (new: joined)    ├─ Contracts ─ Prediction Features
│                 ├─ STAGING ─ NORMALIZATION ┤
└─ **Contracts** ─┘                          └─ dbt MARTS
  (NEW)           (raw data) (processed)
```

---

## Critical Features for Prediction

### Contract-Level Indicators:
1. **Guaranteed Money %** - Higher guaranteed money = higher dead cap risk (70%+ guarantee = immediate dead cap if cut)
2. **Signing Bonus** - Lump sum prorated over contract years (key dead cap source)
3. **Contract Length** - Longer contracts = more flexibility for front office to absorb dead cap
4. **Years Remaining** - Player in year 3+ of contract = higher restructure/cut risk

### Player-Level Indicators:
1. **Age** - Players 32+ have higher cut/dead money risk
2. **Performance (AV)** - Declining AV → increased dead cap
3. **Experience** - Veteran players (10+ years) more likely to be cut
4. **Games Played** - Injury indicators (low games = cut/injury risk)

### Team-Level Indicators:
1. **Team Dead Money %** - Historical team cap discipline
2. **Position** - WR, RB positions have higher dead money rates

---

## Data Quality & Assumptions

### Known Limitations:
- **Spotrac team contracts pages** may not include all contract details (guaranteed, signing bonus values sometimes missing)
- **PFR data completeness** varies by year (pre-2000 data sparse)
- **Name matching** is case-insensitive but could have false negatives (nicknames, name changes)
- **Seasonal data** - Dead money is announced offseason; lags actual transaction dates

### Design Decisions:
- **Keep synthetic players** in dataset - flagged but not filtered (intentional)
- **Left joins** for roster/contract data - gracefully handles missing enrichment
- **Allow NULLs in derived features** - modeling layer handles missing data

---

## Running the Pipeline

### Manual Testing:
```bash
# Scrape contracts for 2024
PYTHONPATH=. ./.venv/bin/python src/spotrac_scraper_v2.py player-contracts 2024

# Stage contracts
PYTHONPATH=. ./.venv/bin/python src/ingestion.py --source spotrac-contracts --year 2024

# Normalize with enrichment
PYTHONPATH=. ./.venv/bin/python src/normalization.py --year 2024

# Run dbt transformations
./.venv/bin/dbt run --project-dir ./dbt --profiles-dir ./dbt --select tag:contracts
./.venv/bin/dbt run --project-dir ./dbt --profiles-dir ./dbt --select tag:prediction
```

### Via Airflow:
```bash
# Trigger pipeline for 2024
airflow dags trigger nfl_dead_money_pipeline --conf '{"pipeline_year": 2024}'
```

---

## Next Steps for ML Model Development

1. **Feature Engineering**: Add derived features (guaranteed/cap_hit ratio, age*years_remaining, etc.)
2. **Target Definition**: Define "dead money event" (cut within 1 year, restructure, etc.)
3. **Training Data**: Use 2015-2023 historical data with dead money outcomes as labels
4. **Model Selection**: Try DecisionTreeRegressor, GradientBoosting, RandomForest
5. **Validation**: Test on 2024+ data (holdout test set)

---

## Files Modified/Created

### New Files:
- `dbt/models/staging/stg_player_contracts.sql`
- `dbt/models/staging/stg_player_rosters.sql`
- `dbt/models/marts/fct_player_dead_money_features.sql`

### Modified Files:
- `src/spotrac_scraper_v2.py` - Added contract scraper methods
- `src/normalization.py` - Added contract enrichment, roster joining
- `src/ingestion.py` - Added contract staging function
- `dags/nfl_dead_money_pipeline.py` - Updated DAG tasks & dependencies

---

## Verification Checklist

✅ Contract scraper extracts guaranteed money, signing bonus, contract length
✅ PFR roster data (age, performance) joined to player records
✅ dbt models compile without errors
✅ Airflow DAG updated with new tasks
✅ Ingestion layer handles contracts
✅ Normalization joins contract + roster + dead money data
✅ No syntax errors in Python files
✅ CLI commands work for manual testing

---

## Prediction Model Readiness

**Current State**: Data enriched and structure ready
- ✅ Contract financial data available
- ✅ Player age/performance metrics linked
- ✅ Target variable (dead money indicator) available
- ✅ dbt mart provides clean ML-ready table

**Path Forward**:
1. Backfill historical data (2015-2024)
2. Define target variable precisely (next-year dead money vs. cut vs. restructure)
3. Build scikit-learn Decision Tree/RandomForest model in notebook
4. Validate on holdout 2024-2025 data
5. Consider team-level features for additional predictive power
