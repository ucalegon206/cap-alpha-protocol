# Data Structure & Schema Guide

## End-to-End Data Flow

```
┌──────────────────────────────────────────────────────────────────────┐
│ LAYER 1: RAW DATA (Web Scraping)                                    │
└──────────────────────────────────────────────────────────────────────┘
  ├─ data/raw/spotrac_team_cap_2024_*.csv
  │  └─ Columns: team, year, active_cap_millions, dead_money_millions, ...
  │
  ├─ data/raw/spotrac_player_rankings_2024_*.csv
  │  └─ Columns: player_name, team, position, cap_hit_millions, ...
  │
  ├─ data/raw/spotrac_player_contracts_2024_*.csv (NEW!)
  │  └─ Columns: player_name, team, position, total_contract_value_millions,
  │                guaranteed_money_millions, signing_bonus_millions, ...
  │
  └─ data/raw/pfr_rosters_2024_*.csv
     └─ Columns: Player, team, Pos, Age, G, GS, AV, Yrs, College, ...
```

## New Data Sources

### Contract Scraper Added
- **File**: `spotrac_player_contracts_2024_*.csv`
- **Records**: ~1,500-2,000 per year
- **Columns**: player_name, team, position, total_contract_value_millions, guaranteed_money_millions, signing_bonus_millions, contract_length_years, years_remaining, cap_hit_millions, dead_cap_millions
- **Key Features**: Guaranteed %, derived risk categories

### Features Mart Created
- **File**: `fct_player_dead_money_features` (DuckDB table)
- **Records**: ~150-200 per year (dead money cases)
- **Columns**: 28 features including contract + roster + categorical
- **Purpose**: ML-ready table for dead money prediction

## Feature Set

| Category | Count | Key Features |
|----------|-------|--------------|
| Contract Financial | 7 | guaranteed_money, signing_bonus, guaranteed_pct, cap_hit, contract_length, years_remaining |
| Player Performance | 4 | age_at_signing, games_played_prior_year, performance_av, years_experience |
| Categorical Risk | 4 | guarantee_category, age_category, contract_length_category, performance_category |
| Target Variables | 2 | became_dead_money_next_year (binary), dead_money_amount (continuous) |

## File Locations

```
data/processed/compensation/dead_money_features_2024.csv  ← ML-ready training data
nfl_dead_money.duckdb → marts.fct_player_dead_money_features  ← Query-able mart
```

## Query Example

```sql
SELECT *
FROM marts.fct_player_dead_money_features
WHERE guarantee_category = 'high_guarantee'
  AND age_category = 'veteran'
  AND year = 2024;
```

## For More Details

- [DATA_ENRICHMENT_IMPLEMENTATION.md](DATA_ENRICHMENT_IMPLEMENTATION.md) - Technical architecture
- [PREDICTION_FEATURE_SET.md](PREDICTION_FEATURE_SET.md) - Feature definitions & ML guidance
- [TESTING_CONTRACT_ENRICHMENT.md](TESTING_CONTRACT_ENRICHMENT.md) - Step-by-step testing
- [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) - Executive overview
