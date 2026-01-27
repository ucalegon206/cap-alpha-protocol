# Quick Start: Testing Contract Scraper & Enrichment

## Prerequisites

Ensure you're in the project root with venv activated:

```bash
cd /Users/andrewsmith/Documents/portfolio/nfl-dead-money
source .venv/bin/activate
```

---

## Step 1: Test Contract Scraper

Scrape player contracts for a single year (e.g., 2024):

```bash
PYTHONPATH=. python src/spotrac_scraper_v2.py player-contracts 2024
```

**Expected Output**:
```
Scraping player contracts for 2024 (32 teams)
  → ARI: https://www.spotrac.com/nfl/team/ari/contracts/
  → ATL: ...
  ✓ Extracted 50 contracts for ARI
  ✓ Extracted 48 contracts for ATL
  ...
✓ Saved 1,547 records to data/raw/spotrac_player_contracts_2024_20250125_142530.csv
```

**Success Criteria**:
- ✅ File created in `data/raw/`
- ✅ ≥1000 records (all 32 teams combined)
- ✅ Columns: player_name, team, position, total_contract_value_millions, guaranteed_money_millions, etc.
- ✅ No null player_names

---

## Step 2: Test Ingestion

Stage the raw contracts into the staging layer:

```bash
PYTHONPATH=. python src/ingestion.py --source spotrac-contracts --year 2024
```

**Expected Output**:
```
Staged player contracts: data/staging/stg_spotrac_player_contracts_2024.csv (1547 rows)
✓ Ingestion complete for spotrac-contracts (2024)
```

**Success Criteria**:
- ✅ File created in `data/staging/`
- ✅ Row count matches (or close to) scraper output
- ✅ Columns normalized to snake_case

---

## Step 3: Test Normalization with Enrichment

Normalize contracts and join with PFR roster data:

```bash
PYTHONPATH=. python src/normalization.py --year 2024
```

**Expected Output**:
```
Normalizing data for 2024
Normalized team cap → data/processed/compensation/stg_team_cap_2024.csv (32 rows)
Normalized player rankings → data/processed/compensation/stg_player_rankings_2024.csv (500+ rows)
Normalized dead money → data/processed/compensation/stg_dead_money_2024.csv (150+ rows)
Normalized contracts → data/processed/compensation/stg_player_contracts_2024.csv (1500+ rows)
  ✓ Enriched 800+ players with roster data
Normalized dead money features → data/processed/compensation/dead_money_features_2024.csv (150+ rows)
  ✓ Joined 145 contracts to dead money
✓ Normalization complete for 2024
```

**Success Criteria**:
- ✅ `stg_player_contracts_2024.csv` created with ≥1000 rows
- ✅ `dead_money_features_2024.csv` created with enriched data
- ✅ Age, AV columns populated for joined players
- ✅ `guaranteed_pct` calculated

---

## Step 4: Test dbt Models

Seed and run dbt transformations:

```bash
# Load seed data (if needed)
.venv/bin/dbt seed --project-dir ./dbt --profiles-dir ./dbt

# Run staging models (including new contracts model)
.venv/bin/dbt run --project-dir ./dbt --profiles-dir ./dbt --select tag:staging

# Run marts (including new prediction feature model)
.venv/bin/dbt run --project-dir ./dbt --profiles-dir ./dbt --select tag:mart
```

**Expected Output**:
```
Running with dbt 1.x
Found 15 models...
Running 15 models...

    ✓ stg_player_contracts [CREATED TABLE]
    ✓ stg_player_rosters [CREATED TABLE]
    ✓ fct_player_dead_money_features [CREATED TABLE]
    ...

✓ Done: 15 models in 2.5s
```

**Success Criteria**:
- ✅ Models compile without errors
- ✅ `stg_player_contracts` table created
- ✅ `fct_player_dead_money_features` mart created with ≥100 rows
- ✅ Feature columns present (guaranteed_pct, age_at_signing, performance_av, etc.)

---

## Step 5: Inspect Feature Data

Check the output feature table:

```bash
# Quick CSV peek
head -5 data/processed/compensation/dead_money_features_2024.csv | cut -d',' -f1-10

# Full inspection in Python
python -c "
import pandas as pd
df = pd.read_csv('data/processed/compensation/dead_money_features_2024.csv')
print(f'Shape: {df.shape}')
print(f'Columns: {list(df.columns)}')
print(f'Sample rows:')
print(df[['player_name', 'team', 'guaranteed_money_millions', 'age_at_signing', 'performance_av', 'became_dead_money_next_year']].head(10))
print(f'Data types:')
print(df.dtypes)
"
```

**Expected Output**:
```
Shape: (145, 28)
Columns: ['player_name', 'team', 'position', 'year', 'total_contract_value_millions', 
          'guaranteed_money_millions', 'signing_bonus_millions', 'contract_length_years', 
          'years_remaining', 'guaranteed_pct', 'age_at_signing', 'games_played_prior_year', 
          'performance_av', 'years_experience', 'guarantee_category', 'contract_length_category', 
          'age_category', 'performance_category', 'became_dead_money_next_year', 'dead_money_amount']

Sample rows:
       player_name team  guaranteed_money  age_at_signing  performance_av  became_dead
0    Saquon Barkley   NYG            20.5              26              8.0            0
1    Deshaun Watson   CLE            45.0              27             12.0            1
...
```

---

## Step 6: Test Airflow DAG (Optional)

If Airflow is running:

```bash
# Trigger the pipeline for 2024
airflow dags trigger nfl_dead_money_pipeline --conf '{"pipeline_year": 2024}'

# Monitor
airflow dags list-runs nfl_dead_money_pipeline
```

Monitor in Airflow UI (usually at http://localhost:8080):
- Watch `scrape_spotrac_player_contracts` task
- Verify `stage_spotrac_contracts` runs after
- Verify `normalize_data` includes contract processing
- Verify `dbt_run_marts` creates prediction feature table

---

## Troubleshooting

### Issue: Contract scraper times out
**Solution**: Rate limiting is built in (1-sec delay between teams). If timeout occurs:
- Check internet connection
- Verify Spotrac is accessible (not blocked by firewall)
- Try individual team: `https://www.spotrac.com/nfl/team/ari/contracts/`

### Issue: Normalization shows 0 enriched players
**Solution**: PFR roster file may be missing or year mismatch
```bash
# Check for roster files
find data/processed/rosters -name "*2024*"

# If missing, check if it was scraped
find data/raw -name "*roster*" -o -name "*pfr*"
```

### Issue: dbt models fail to compile
**Solution**: Check dbt_project.yml and source definitions
```bash
.venv/bin/dbt parse --project-dir ./dbt --profiles-dir ./dbt  # Show parse errors
.venv/bin/dbt test --project-dir ./dbt --profiles-dir ./dbt   # Run data tests
```

### Issue: Feature values are null
**Solution**: Join key mismatch (player_name variation). Check:
```bash
python -c "
import pandas as pd
contracts = pd.read_csv('data/processed/compensation/stg_player_contracts_2024.csv')
rosters = pd.read_csv('data/processed/rosters/pfr_combined_rosters.csv')
rosters_2024 = rosters[rosters['year'] == 2024]
print(f'Contracts: {len(contracts)} rows')
print(f'Rosters 2024: {len(rosters_2024)} rows')
print(f'Sample contract names: {contracts[\"player_name\"].head()}')
print(f'Sample roster names: {rosters_2024[\"Player\"].head()}')
"
```

---

## Next: Build Prediction Model

Once features are validated, create a modeling notebook:

```python
# notebooks/prediction_model.ipynb
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

# Load features
features = pd.read_csv('data/processed/compensation/dead_money_features_2024.csv')

# Define X, y
X = features[['guaranteed_pct', 'contract_length_years', 'age_at_signing', 
              'performance_av', 'years_experience']]
y = features['became_dead_money_next_year']

# Split & train
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
model = RandomForestRegressor()
model.fit(X_train, y_train)

# Evaluate
from sklearn.metrics import mean_squared_error, r2_score
pred = model.predict(X_test)
print(f"R² Score: {r2_score(y_test, pred):.3f}")
print(f"Feature Importance:\n{pd.Series(model.feature_importances_, index=X.columns).sort_values(ascending=False)}")
```

---

## Verification Checklist

- [ ] Contract scraper runs without errors (2024)
- [ ] Ingestion creates staging file with ≥1000 rows
- [ ] Normalization enriches players with age/AV data
- [ ] dbt models compile successfully
- [ ] Feature table has ≥100 rows
- [ ] `guaranteed_pct` column populated
- [ ] `became_dead_money_next_year` target variable present
- [ ] Spot-check: High guaranteed contracts have dead_money = 1

---

## Files to Monitor

| File | Purpose | Expected Size |
|------|---------|---------------|
| `data/raw/spotrac_player_contracts_2024_*.csv` | Raw scrape | ~1500 rows |
| `data/staging/stg_spotrac_player_contracts_2024.csv` | Staged | ~1500 rows |
| `data/processed/compensation/stg_player_contracts_2024.csv` | Normalized | ~1500 rows |
| `data/processed/compensation/dead_money_features_2024.csv` | Enriched ML-ready | ~150 rows |
| DuckDB `nfl_dead_money.duckdb` | dbt mart | Schema `marts` tables |
