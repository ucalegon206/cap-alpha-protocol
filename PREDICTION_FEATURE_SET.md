# Dead Money Prediction Feature Set

## Overview

The `fct_player_dead_money_features` dbt mart provides a complete ML-ready dataset combining:
- **Contract financial terms** (guaranteed money, signing bonus, structure)
- **Player performance metrics** (age, experience, games, approximate value)
- **Historical dead money outcomes** (binary target variable)

---

## Features by Category

### 1. Contract Financial Features

| Feature | Type | Description | Prediction Value |
|---------|------|-------------|------------------|
| `total_contract_value_millions` | Decimal | Total contract value | ⭐⭐⭐ High variance indicator |
| `guaranteed_money_millions` | Decimal | Guaranteed portion | ⭐⭐⭐⭐⭐ Directly determines dead cap |
| `signing_bonus_millions` | Decimal | Signing bonus lump sum | ⭐⭐⭐⭐⭐ Prorated = dead cap |
| `contract_length_years` | Integer | Years of contract | ⭐⭐⭐⭐ Flexibility indicator |
| `years_remaining` | Integer | Years left on deal | ⭐⭐⭐⭐ Current risk window |
| `cap_hit_millions` | Decimal | Current year salary cap hit | ⭐⭐⭐ Current financial burden |
| `guaranteed_pct` | Decimal (0-100) | Guaranteed / Total value % | ⭐⭐⭐⭐⭐ Risk multiplier |

### 2. Player Performance Features

| Feature | Type | Description | Prediction Value |
|---------|------|-------------|------------------|
| `age_at_signing` | Integer | Player age at contract signing | ⭐⭐⭐⭐ Age = decline risk |
| `games_played_prior_year` | Integer | Games played in prior season | ⭐⭐⭐ Health indicator |
| `performance_av` | Decimal | Approximate Value (PFR metric) | ⭐⭐⭐ Performance decline → cut |
| `years_experience` | Integer | Seasons in NFL | ⭐⭐⭐⭐ Veterans = cut risk |

### 3. Position Features (from raw data)

| Feature | Type | Description | Prediction Value |
|---------|------|-------------|------------------|
| `position` | String | QB, RB, WR, TE, OL, DL, LB, DB | ⭐⭐⭐ Position-specific risk profiles |

### 4. Derived Categorical Features

**For tree-based model interpretability**:

| Category | Levels | Example | Rationale |
|----------|--------|---------|-----------|
| `guarantee_category` | high (>80%), moderate (50-80%), low (<50%) | "high_guarantee" | 80%+ guarantee = near-certain dead cap if cut |
| `contract_length_category` | long-term (3+ yrs), medium-term (2 yrs), short-term (1 yr) | "long_term" | Longer = less restructure flexibility |
| `age_category` | veteran (32+), prime (27-31), young (<27) | "veteran" | Clear age-based injury/decline risk |
| `performance_category` | elite (AV>10), good (5-10), average (2-5), below_avg (<2) | "elite" | Below-average = cut candidate |

---

## Target Variable

| Name | Type | Description | Values |
|------|------|-------------|--------|
| `became_dead_money_next_year` | Binary (0/1) | Did player have dead money next year? | 0 = No, 1 = Yes |
| `dead_money_amount` | Decimal | Actual dead money amount (if any) | $M in dead cap |

---

## Example Feature Combinations & Risk Profiles

### High Risk Scenario 1: Aging Star with Large Guarantee
```
guaranteed_pct:     85% (high_guarantee)
contract_length:    3 years (long_term)
age_category:       veteran (35 years old)
performance_av:     6 (good but declining)
signing_bonus:      $15M (prorated = ~$5M/year dead cap)
→ Prediction: HIGH DEAD MONEY RISK (likely cut year 2-3)
```

### High Risk Scenario 2: Injury-Prone Mid-Tier
```
guaranteed_pct:     60% (moderate_guarantee)
games_played:       6 (well below 16-game threshold)
age_category:       prime (29 years old)
performance_av:     3 (average, declining)
years_remaining:    2
→ Prediction: MODERATE DEAD MONEY RISK (restructure or cut possibility)
```

### Low Risk Scenario: Young Performer on Short Deal
```
guaranteed_pct:     40% (low_guarantee)
age_category:       young (24 years old)
performance_av:     9 (elite)
years_remaining:    1
contract_length:    1 year (short_term)
→ Prediction: LOW DEAD MONEY RISK (high commodity value, likely retained)
```

---

## Data Availability by Position

| Position | Feature Completeness | Notes |
|----------|---------------------|-------|
| QB | ✅ 95%+ | Most complete contracts, age data |
| RB | ⚠️ 75% | Some roster gaps for older players |
| WR | ✅ 90%+ | Good Spotrac coverage |
| TE | ⚠️ 80% | Similar to WR |
| OL | ⚠️ 70% | Fewer PFR performance metrics |
| DL | ⚠️ 75% | Mixed roster/contract coverage |
| LB | ⚠️ 75% | Similar to DL |
| DB | ⚠️ 80% | Good contract data, variable performance |

---

## Feature Engineering Recommendations

### Interaction Features
```python
# These might improve prediction:
guaranteed_signed_bonus_ratio = signing_bonus / guaranteed_money
contract_value_per_year = total_contract_value / contract_length
age_contract_duration = age_at_signing + contract_length_years
guaranteed_cap_burden = guaranteed_money / total_contract_value
```

### Temporal Features
```python
# From current year context:
seasons_into_contract = contract_length_years - years_remaining
performance_trend = current_av - prior_av  # if multi-year data
```

### Position-Based Features
```python
# Position-specific risk profiles:
position_avg_dead_money_rate_league
position_avg_guaranteed_pct_league
position_typical_age_at_cut
```

---

## Modeling Approaches

### 1. Binary Classification (Dead Money Yes/No)
```
Target: became_dead_money_next_year
Model: DecisionTreeClassifier, RandomForestClassifier, LogisticRegression
Features: All contract + roster features
Evaluation: Precision/Recall (important: few false negatives)
```

### 2. Regression (Dead Money Amount)
```
Target: dead_money_amount
Model: DecisionTreeRegressor, GradientBoostingRegressor
Features: All contract + roster features
Evaluation: MAE, RMSE (focus on outliers: high dead money cases)
```

### 3. Multi-Class (Dead Money Tier)
```
Target: dead_money_category = Low (<$5M), Medium ($5-15M), High (>$15M)
Model: RandomForestClassifier with class weights
Features: All features
Evaluation: F1 per class (balanced accuracy)
```

---

## SQL Query: Export Features for ML

```sql
SELECT
    player_name,
    team,
    position,
    year,
    -- Contract features
    total_contract_value_millions,
    guaranteed_money_millions,
    signing_bonus_millions,
    contract_length_years,
    years_remaining,
    guaranteed_pct,
    cap_hit_millions,
    -- Roster features
    age_at_signing,
    games_played_prior_year,
    performance_av,
    years_experience,
    -- Categorical features
    guarantee_category,
    contract_length_category,
    age_category,
    performance_category,
    -- Target
    became_dead_money_next_year,
    dead_money_amount
FROM marts.fct_player_dead_money_features
WHERE year BETWEEN 2018 AND 2023  -- Training data
  AND total_contract_value_millions > 0
  AND guaranteed_money_millions > 0
ORDER BY year, team, player_name;
```

---

## Data Quality Expectations

### Feature Completeness
- **Guaranteed Money**: ~85% populated (Spotrac may not have all details)
- **Age/Performance**: ~90% (PFR roster join, some gaps)
- **Contract Length**: ~80% (inferred from years_remaining on some)
- **Dead Money Target**: ~95% (captured from Spotrac dead money pages)

### Outliers to Monitor
- **Signing Bonuses > $50M**: Legitimate but rare (elite QBs, RBs)
- **Guaranteed % > 95%**: Unusual, check data quality
- **Age < 20 or > 40**: Valid but rare, may indicate data entry errors
- **AV = 0**: Valid (rookies, practice squad), not errors

---

## Integration with Notebook Analysis

The feature set is designed for use in Jupyter notebooks:

```python
import pandas as pd
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split

# Load from dbt
features_df = pd.read_csv('data/processed/marts/fct_player_dead_money_features.csv')

# Prepare for modeling
X = features_df[[
    'total_contract_value_millions', 'guaranteed_pct', 'contract_length_years',
    'age_at_signing', 'performance_av', 'years_experience'
]]
y = features_df['became_dead_money_next_year']

# Train/test split (by year for time-series integrity)
train = features_df[features_df['year'] <= 2022]
test = features_df[features_df['year'] > 2022]

X_train = train[X.columns]
y_train = train['became_dead_money_next_year']

X_test = test[X.columns]
y_test = test['became_dead_money_next_year']

# Model
model = DecisionTreeRegressor(max_depth=5)
model.fit(X_train, y_train)

# Predict on holdout
predictions = model.predict(X_test)
```

---

## Success Metrics

A working prediction model should:

1. **Precision ≥ 70%** - When model says "high dead money risk," it's right 7/10 times
2. **Recall ≥ 60%** - Catch 6/10 of actual dead money cases
3. **Feature Importance**: Guarantee % and signing bonus rank in top 3
4. **Interpretability**: Tree depth ≤ 5 for human-readable decision rules

Example rule the model might learn:
> *"IF guaranteed_pct > 75% AND age_at_signing > 30 AND years_remaining <= 2, THEN high dead money risk (85% probability)"*
