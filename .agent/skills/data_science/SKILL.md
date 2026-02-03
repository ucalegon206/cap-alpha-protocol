---
name: Data Science
description: Standards for Data Science workflows, from exploration to feature engineering.
---

# Data Science Standards

## 1. Exploration vs. Production
- **Notebooks**: Use `.ipynb` ONLY for initial EDA (Exploratory Data Analysis) and visualization.
- **Scripts**: All logic destined for the pipeline must be refactored into `.py` modules (e.g., `src/features/`).
- **Rule**: Never run a notebook in a production DAG.

## 2. Feature Engineering Principles
- **Point-in-Time Correctness**: Features for a given row (Player-Week) must ONLY use data available *prior* to that game.
  - *Example*: Calculating "Next Week's Salary" valid? NO. 
  - *Example*: Calculating "Avg Score Last 3 Weeks"? YES.
- **Leakage Prevention**: Strictly split training/validation by TIME (e.g., Train 2011-2022, Valid 2023), never random shuffle, due to time-series correlations.

## 3. Metrics that Matter
- **Business Proxy**: Always prioritize metrics that map to business value (e.g., "Financial Lift", "Dead Cap Saved") over raw technical metrics (MSE, Accuracy) unless debugging.
- **Baselines**: Always compare model performance against a naive baseline (e.g., "Predict last year's performance").
