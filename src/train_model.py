
import pandas as pd
import numpy as np
import duckdb
import logging
import xgboost as xgb
import shap
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_PATH = "data/nfl_data.db"
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

class RiskModeler:
    def __init__(self, db_path=DB_PATH):
        self.con = duckdb.connect(db_path)

    def prepare_data(self, target_col='edce_risk'):
        logger.info(f"Loading feature matrix for target: {target_col}...")
        df = self.con.execute("SELECT * FROM staging_feature_matrix").df()
        
        # 1. Split into features and target
        skip_cols = ['player_name', 'year', 'experience_years', 'edce_risk', 'fair_market_value', 'ied_overpayment', 'value_metric_proxy']
        X = df.drop(columns=[c for c in skip_cols if c in df.columns])
        
        # Robust numeric conversion
        X = X.apply(pd.to_numeric, errors='coerce').dropna(axis=1, how='all').fillna(0)
        y = df[target_col].fillna(0)
        
        # 2. Retain player info for joining results back
        metadata = df[['player_name', 'year', 'team']]
        
        return X, y, metadata

    def train_xgboost(self, X, y):
        logger.info("Training Production XGBoost Model...")
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Hyperparameters for transparency and stability
        model = xgb.XGBRegressor(
            n_estimators=1000,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            n_jobs=-1,
            random_state=42,
            early_stopping_rounds=50
        )
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )
        
        preds = model.predict(X_test)
        rmse = np.sqrt(mean_squared_error(y_test, preds))
        r2 = r2_score(y_test, preds)
        
        logger.info(f"✓ Model Trained. RMSE: {rmse:.4f}, R2: {r2:.4f}")
        return model, X_test

    def generate_shap_report(self, model, X_test):
        logger.info("Generating SHAP Transparency Explainer...")
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X_test)
        
        plt.figure(figsize=(10, 6))
        shap.summary_plot(shap_values, X_test, show=False)
        plt.tight_layout()
        plt.savefig("reports/shap_summary.png")
        logger.info("✓ SHAP summary plot saved to reports/shap_summary.png")
        
        return shap_values

    def save_predictions(self, model, X, metadata):
        logger.info("Saving Predictions to DuckDB...")
        preds = model.predict(X)
        metadata['predicted_risk_score'] = preds
        
        self.con.execute("CREATE OR REPLACE TABLE prediction_results AS SELECT * FROM metadata")
        logger.info("✓ Predictions persisted to 'prediction_results' table.")

if __name__ == "__main__":
    modeler = RiskModeler()
    X, y, metadata = modeler.prepare_data()
    model, X_test = modeler.train_xgboost(X, y)
    modeler.generate_shap_report(model, X_test)
    modeler.save_predictions(model, X, metadata)
