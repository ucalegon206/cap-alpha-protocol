
import subprocess
import logging
import sys
from pathlib import Path
from src.strategic_engine import StrategicEngine

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_step(name, command):
    logger.info(f"--- Starting Step: {name} ---")
    try:
        # Using sys.executable to ensure we use the same venv
        full_command = f"export PYTHONPATH=$PYTHONPATH:. && {sys.executable} {command}"
        result = subprocess.run(full_command, shell=True, check=True)
        logger.info(f"--- Completed Step: {name} ---")
    except subprocess.CalledProcessError as e:
        logger.error(f"--- FAILED Step: {name} (Exit Code: {e.returncode}) ---")
        sys.exit(e.returncode)

def main():
    logger.info("NFL Dead Money Pipeline: Hardened End-to-End Execution")
    
    # 1. Ingestion & Normalization
    run_step("Data Ingestion", "scripts/ingest_to_duckdb.py")
    
    # 2. Quality Gate (Pre-Modeling)
    run_step("Data Validation", "scripts/validate_gold_layer.py")
    
    # 3. Feature Engineering
    run_step("Feature Factory", "src/feature_factory.py")
    
    # 4. Model Training & Risk Frontier (Production XGBoost)
    run_step("Production Training", "src/train_model.py")
    
    # 5. Pipeline Integrity Testing (Formal pytest)
    # We skip Selenium-based tests for speed during standard pipeline runs
    run_step("Integrity Testing", "-m pytest tests/test_strategic_engine.py tests/test_data_integrity.py")
    
    # 6. Strategic Intelligence & Audits
    logger.info("--- Starting Step: Strategic Audits ---")
    try:
        engine = StrategicEngine("data/nfl_data.db")
        engine.generate_audit_report("reports/nfl_team_strategic_audit_2025.md", year=2025)
        # Explicitly close to release DuckDB lock
        engine.close()
        logger.info("--- Completed Step: Strategic Audits ---")
    except Exception as e:
        logger.error(f"--- FAILED Step: Strategic Audits ({e}) ---")
        sys.exit(1)

    # 7. Supplemental Reports
    run_step("Super Bowl Audit", "scripts/generate_sb_audit.py")
    run_step("Intelligence Report", "scripts/generate_intelligence_report.py")
    
    logger.info("✓ Pipeline Execution Successful. Production Artifacts Updated.")

if __name__ == "__main__":
    main()
