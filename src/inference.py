
import pandas as pd
import duckdb
import logging
import joblib
import os
from pathlib import Path
from src.feature_factory import FeatureFactory

logger = logging.getLogger(__name__)

class InferenceEngine:
    def __init__(self, db_path: str, model_dir: str = "models"):
        self.db_path = db_path
        self.model_dir = Path(model_dir)

    def get_latest_model(self):
        # Search local and fallback tmp/models
        search_paths = [self.model_dir, Path("/tmp/models")]
        models = []
        for path in search_paths:
            if path.exists():
                models.extend(list(path.glob("xgboost_risk_model_*.pkl")))
        
        if not models:
            return None
        latest_model = max(models, key=os.path.getctime)
        return joblib.load(latest_model)

    def enrich_gold_layer(self):
        """
        Loads the feature matrix, runs inference, and updates fact_player_efficiency.
        """
        factory = FeatureFactory(self.db_path)
        df_features = factory.generate_hyperscale_matrix()
        
        model = self.get_latest_model()
        if model is None:
            logger.warning("No ML model found. Skipping ML enrichment.")
            return

        # Prepare features for inference
        skip_cols = ['player_name', 'year', 'experience_years', 'edce_risk', 'fair_market_value', 'ied_overpayment', 'value_metric_proxy', 'team']
        X = df_features.drop(columns=[c for c in skip_cols if c in df_features.columns])
        # Predict Risk
        meta_path = self.model_dir / "feature_names.json"
        if not meta_path.exists():
            meta_path = Path("/tmp/models/feature_names.json")
            
        if meta_path.exists():
            import json
            with open(meta_path, 'r') as f:
                expected_features = json.load(f)
        else:
            logger.warning("Feature names JSON not found. Using booster fallback.")
            expected_features = model.get_booster().feature_names
            
        if expected_features is None:
             logger.warning("Feature names not found in model metadata. Falling back to input features.")
             expected_features = X.columns
             
        logger.info(f"ML Inference: Expected {len(expected_features)} features")
        
        # Reindex to match expected features, adding missing ones as 0 and dropping extra
        X_aligned = X.reindex(columns=expected_features, fill_value=0)
        
        # FINAL ENSURE: DROP ANY COLUMN NOT IN BOOSTER IF POSSIBLE
        # XGBoost is very picky about column order and count
        df_features['ml_risk_score'] = model.predict(X_aligned)
        
        # Heuristic 2.0: ML-Adjusted Fair Market Value
        # We use a base multiplier on the ML risk to adjust the heuristic FMV
        # If risk is high, FMV should drop more steeply than static linear logic
        df_features['ml_fair_market_value'] = df_features['fair_market_value'] * (1.0 - df_features['ml_risk_score'].clip(0, 1) * 0.5)

        # Persistence
        con = duckdb.connect(self.db_path)
        
        # Update fact_player_efficiency with ML columns
        # First check if columns exist
        con.execute("ALTER TABLE fact_player_efficiency ADD COLUMN IF NOT EXISTS ml_risk_score DOUBLE")
        con.execute("ALTER TABLE fact_player_efficiency ADD COLUMN IF NOT EXISTS ml_fair_market_value DOUBLE")
        
        # Temp table for joining
        con.execute("CREATE OR REPLACE TEMPORARY TABLE ml_updates AS SELECT player_name, team, year, ml_risk_score, ml_fair_market_value FROM df_features")
        
        con.execute("""
            UPDATE fact_player_efficiency
            SET 
                ml_risk_score = ml_updates.ml_risk_score,
                ml_fair_market_value = ml_updates.ml_fair_market_value
            FROM ml_updates
            WHERE fact_player_efficiency.player_name = ml_updates.player_name 
              AND fact_player_efficiency.team = ml_updates.team
              AND fact_player_efficiency.year = ml_updates.year
        """)
        
        logger.info("✓ Gold Layer enriched with ML Intelligence (Risk & FMV 2.0)")
        con.close()

if __name__ == "__main__":
    import os
    db_path = os.getenv("DB_PATH", "data/nfl_data.db")
    engine = InferenceEngine(db_path)
    engine.enrich_gold_layer()
