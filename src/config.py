import yaml
from pathlib import Path
import os

# Define Base Directory (Root of Repo)
# Assumes this file is in src/
BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG_PATH = BASE_DIR / "config/settings.yaml"

def load_config():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

_config = load_config()

# Expose Paths
DATA_RAW_DIR = BASE_DIR / _config["paths"]["data_raw"]
DATA_PROCESSED_DIR = BASE_DIR / _config["paths"]["data_processed"]
MODELS_DIR = BASE_DIR / _config["paths"]["models"]
REPORTS_DIR = BASE_DIR / _config["paths"]["reports"]
VIZ_DIR = BASE_DIR / _config["paths"]["viz"]

# Global Constants
CURRENT_SEASON = _config["project"]["current_season"]

# Create dirs if they don't exist (Safety)
for d in [DATA_RAW_DIR, DATA_PROCESSED_DIR, MODELS_DIR, REPORTS_DIR, VIZ_DIR]:
    d.mkdir(parents=True, exist_ok=True)
