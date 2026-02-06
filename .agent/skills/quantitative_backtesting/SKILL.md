---
name: Quantitative Backtesting
description: Standards for rigorous temporal validation and rolling backtests.
---

# Quantitative Backtesting Standards

## 1. The Golden Rule of Time
**"Thou shalt not peek at the future."**
- Random shuffling (`train_test_split`) is FORBIDDEN for time-dependent data.
- All validation must be **Walk-Forward**: Train on $t_{0} \dots t_{n}$, Test on $t_{n+1}$.

## 2. Walk-Forward Validation (Rolling Window)
Simulate the production reality of predicting the "next" season.

### Fold Structure (Example)
- **Fold 1**: Train [2011-2018] -> Test [2019]
- **Fold 2**: Train [2011-2019] -> Test [2020]
- ...
- **Fold N**: Train [2011-2024] -> Test [2025]

## 3. The "Red Team" Metrics
A model is only valid if it survives the **Temporal Degradation Test**:
- Does RMSE spike in later years? (Architecture rot)
- Does the model fail to adapt to regime changes (e.g., Cap Spikes)?

## 4. Feature Lag Integrity
- Ensure `lag_1` features for Year $Y$ strictly strictly come from Year $Y-1$.
- Any "future" features (e.g., 'career_total_at_retirement') are strictly prohibited.
