
import pandas as pd
import numpy as np
import duckdb
import logging
import xgboost as xgb
try:
    import shap
except ImportError:
    shap = None
import matplotlib.pyplot as plt
import os
from sklearn.metrics import mean_squared_error, r2_score
from pathlib import Path
from src.feature_store import FeatureStore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from src.config_loader import get_db_path

DB_PATH = get_db_path()
MODEL_DIR = Path("/tmp/models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)

class RiskModeler:
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.con = duckdb.connect(db_path)

    def prepare_data(self, target_col='edce_risk'):
        logger.info(f"Loading feature matrix from FEATURE STORE...")
        
        # Initialize store
        store = FeatureStore(db_path=self.db_path)
        
        # Get historical features (diagonal join)
        # Using 2015-2025 range
        df_features = store.get_historical_features(min_year=2015, max_year=2025)
        
        if df_features.empty:
            raise ValueError("Feature store returned empty dataframe. Run materialize_features.py first.")
            
        # Get Target and Metadata from staging (or re-derive)
        logger.info("Loading targets and metadata...")
        df_targets = self.con.execute(f"""
            SELECT player_name, year, team, {target_col}
            FROM fact_player_efficiency
            WHERE year BETWEEN 2015 AND 2025
        """).df()
        
        # Merge Features and Targets
        df = pd.merge(df_targets, df_features, on=['player_name', 'year'], how='inner')
        
        # 1. Split into features and target
        skip_cols = ['player_name', 'year', 'team', target_col]
        X = df.drop(columns=[c for c in skip_cols if c in df.columns])
        
        # Robust numeric conversion
        X = X.apply(pd.to_numeric, errors='coerce').dropna(axis=1, how='all').fillna(0)
        y = df[target_col].fillna(0)
        
        # 2. Retain player info for joining results back
        metadata = df[['player_name', 'year', 'team']]
        
        logger.info(f"✓ Data Prepared: {len(X)} rows, {len(X.columns)} features.")
        return X, y, metadata

    def train_xgboost(self, X, y, metadata):
        logger.info("Training Production XGBoost Model (with Walk-Forward Validation)...")
        
        # 1. Run Backtest First
        from src.backtesting import WalkForwardValidator
        validator = WalkForwardValidator()
        backtest_results = validator.run_backtest(X, y, metadata)
        validator.generate_report(backtest_results)
        
        # 2. Train Final Production Model on ALL History
        # We use all available data to predict the "unknown" future (2025/2026)
        logger.info("Training Final Production Model on full history...")
        
        # Use config params
        try:
             import yaml
             with open("config/ml_config.yaml", "r") as f:
                  config = yaml.safe_load(f)
             params = config["models"]["xgboost"]["params"]
        except Exception as e:
             logger.warning(f"Could not load config: {e}. Using defaults.")
             params = {'n_estimators': 100, 'max_depth': 4, 'learning_rate': 0.1}
        
        model = xgb.XGBRegressor(**params)
        model.fit(X, y, verbose=False)
        
        # 3. Use the latest fold's test set as a proxy for "X_test" for SHAP/Metrics
        # This is strictly for reporting purposes
        latest_year = metadata['year'].max()
        test_mask = metadata['year'] == latest_year
        X_test_proxy = X[test_mask]
        
        logger.info(f"✓ Model Trained on full history ({len(X)} rows).")
        return model, X_test_proxy, backtest_results
    def generate_shap_report(self, model, X_test):
        if shap is None:
            logger.warning("SHAP library not available. Skipping explanation.")
            return None
            
        try:
            logger.info("Generating SHAP Transparency Explainer...")
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(X_test)
            
            plt.figure(figsize=(10, 6))
            shap.summary_plot(shap_values, X_test, show=False)
            plt.tight_layout()
            report_path = os.getenv("REPORT_PATH", "reports/shap_summary.png")
            plt.savefig(report_path, dpi=300, bbox_inches='tight')
            logger.info(f"✓ SHAP summary plot saved to {report_path}")
            return shap_values
        except Exception as e:
            logger.warning(f"SHAP generation failed (optional): {e}")
            return None

    def save_predictions(self, model, X, metadata, metrics):
        import joblib
        import json
        import yaml
        from datetime import datetime
        from src.ml_governance import MLGovernance
        
        logger.info("Saving Predictions and Model Artifacts...")
        
        # 1. Save Predictions to DB
        preds = model.predict(X)
        metadata['predicted_risk_score'] = preds
        self.con.execute("CREATE OR REPLACE TABLE prediction_results AS SELECT * FROM metadata")
        logger.info("✓ Predictions persisted to 'prediction_results' table.")
        
        # 2. Save Model Artifact
        with open("config/ml_config.yaml", "r") as f:
            ml_config = yaml.safe_load(f)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        model_filename = ml_config["models"]["xgboost"]["file_pattern"].format(timestamp=timestamp)
        model_path = MODEL_DIR / model_filename
        joblib.dump(model, model_path)
        logger.info(f"✓ Model artifact saved to: {model_path}")
        
        # 3. Register as Candidate
        governance = MLGovernance()
        governance.register_candidate(
            model_path=model_path,
            metrics=metrics,
            feature_names=list(X.columns)
        )
        logger.info("✓ Model registered in governance registry.")

if __name__ == "__main__":
    modeler = RiskModeler()
    X, y, metadata = modeler.prepare_data()
    model, X_test, backtest_results = modeler.train_xgboost(X, y, metadata)
    
    # Validation against config thresholds using AVERAGE backtest performance
    avg_rmse = backtest_results['rmse'].mean()
    avg_r2 = backtest_results['r2'].mean()
    
    metrics = {
        "rmse": float(avg_rmse),
        "r2": float(avg_r2)
    }
    
    modeler.generate_shap_report(model, X_test)
    modeler.save_predictions(model, X, metadata, metrics)
