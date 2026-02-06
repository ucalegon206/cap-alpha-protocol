
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
    import argparse
    parser = argparse.ArgumentParser(description="NFL Dead Money Pipeline Orchestrator")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip Bronze Layer Ingestion (2011-2025)")
    parser.add_argument("--skip-validation", action="store_true", help="Skip Gold Layer Validation")
    parser.add_argument("--skip-features", action="store_true", help="Skip Feature Engineering")
    parser.add_argument("--skip-training", action="store_true", help="Skip Model Training")
    parser.add_argument("--skip-tests", action="store_true", help="Skip Integrity Tests")
    parser.add_argument("--skip-audits", action="store_true", help="Skip Strategic Audits & Reports")
    args = parser.parse_args()

    logger.info(f"--- Pipeline Started with Options: {args} ---")
    
    # 1. Ingestion & Normalization (Bronze/Silver)
    if not args.skip_ingest:
        for year in range(2011, 2026):
             run_step(f"Ingestion {year}", f"scripts/ingest_to_duckdb.py --year {year}")
    else:
        logger.info("⏭️  Skipping Ingestion (Bronze Layer)")
    
    # 2. Quality Gate (Silver/Gold Check)
    if not args.skip_validation:
        run_step("Data Validation", "scripts/validate_gold_layer.py")
    else:
        logger.info("⏭️  Skipping Validation (Quality Gate)")
    
    # 3. Feature Engineering (Gold Layer Construction)
    if not args.skip_features:
        run_step("Feature Factory", "src/feature_factory.py")
    else:
        logger.info("⏭️  Skipping Feature Engineering (Gold Layer)")
    
    # 4. Model Training & Risk Frontier (Production XGBoost)
    if not args.skip_training:
        run_step("Production Training", "src/train_model.py")
    else:
        logger.info("⏭️  Skipping Training (Model Layer)")
    
    # 5. Pipeline Integrity Testing (Formal pytest)
    if not args.skip_tests:
        run_step("Integrity Testing", "-m pytest tests/test_strategic_engine.py tests/test_data_integrity.py")
    else:
        logger.info("⏭️  Skipping Tests (Integrity Layer)")
    
    # 6. Strategic Intelligence & Audits
    if not args.skip_audits:
        logger.info("--- Starting Step: Strategic Audits ---")
        try:
            import os
            db_path = os.getenv("DB_PATH", "data/nfl_belichick.db")
            engine = StrategicEngine(db_path)
            report_path = os.getenv("AUDIT_REPORT_PATH", "reports/nfl_team_strategic_audit_2025.md")
            engine.generate_audit_report(report_path, year=2025)
            engine.close()
            logger.info("--- Completed Step: Strategic Audits ---")
        except Exception as e:
            logger.error(f"--- FAILED Step: Strategic Audits ({e}) ---")
            sys.exit(1)

        # 7. Supplemental Reports
        run_step("Super Bowl Audit", "scripts/generate_sb_audit.py")
        run_step("Intelligence Report", "scripts/generate_intelligence_report.py")
    else:
         logger.info("⏭️  Skipping Audits (Reporting Layer)")
    
    logger.info("✓ Pipeline Execution Successful. Artifacts Updated.")

if __name__ == "__main__":
    main()
