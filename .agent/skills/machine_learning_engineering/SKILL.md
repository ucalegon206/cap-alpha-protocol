---
name: Machine Learning Engineering
description: Principles for building reproducible, deployable ML systems.
---

# Machine Learning Engineering Principles

## 1. Reproducibility
- **Seeds**: Always set random seeds for numpy, pandas, and the model framework (XGBoost/sklearn).
- **Config**: Model hyperparameters must be decoupled from code (passed via args or config file), not hardcoded.

## 2. Artifact Management
- **Model Saving**: Save trained models with versioning info.
  - Path: `models/{model_type}_{timestamp}.pkl`
- **Metadata**: Save a companion JSON with the model containing:
  - Feature list (critical for inference alignment)
  - Training date
  - Performance metrics on validation set

## 3. Inference Pattern
- **Batch Inference**: For this pipeline, prefer batch inference scripts (`src/predict.py`) that read from the Feature Store (DuckDB) and write predictions back to a Results table.
- **Validation**: "Train-Serve Skew" checks. Ensure the distribution of features at inference time matches training time.
