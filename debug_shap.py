
import sys
import os
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'libs'))
sys.path.append(os.path.join(os.getcwd(), 'src'))
import joblib
import pandas as pd
import numpy as np
import traceback
import xgboost as xgb
import glob

# PATCH NUMPY FOR SHAP COMPATIBILITY
print(f"Numpy version: {np.__version__}")
print(f"Numpy file: {np.__file__}")
try:
    if not hasattr(np, "_ARRAY_API"):
        np._ARRAY_API = False
    if not hasattr(np, "obj2sctype"):
        np.obj2sctype = lambda x: np.dtype(x).type
    print("Patched numpy for SHAP")
except Exception as e:
    print(f"Patch failed: {e}")

import shap

def debug_shap():
    print("Loading model...")
    # Find latest model
    model_dir = "/tmp/models"
    models = sorted(glob.glob(os.path.join(model_dir, "*.pkl")))
    if not models:
        print("No models found!")
        return
    
    latest_model_path = models[-1]
    print(f"Loading {latest_model_path}")
    model = joblib.load(latest_model_path)
    
    print("Inspecting model attributes...")
    try:
        if hasattr(model, 'feature_names_in_'):
            features = model.feature_names_in_
            print(f"Features in model: {len(features)}")
        else:
            print("Model lacks feature_names_in_")
            features = [f"f{i}" for i in range(model.n_features_in_)]
    except Exception as e:
        print(f"Failed to get features: {e}")
        features = [f"f{i}" for i in range(10)] # Fallback

    # Create dummy X
    X = pd.DataFrame(np.random.rand(10, len(features)), columns=features)
    
    print("Attempting SHAP TreeExplainer...")
    try:
        explainer = shap.TreeExplainer(model)
        shap_values = explainer.shap_values(X)
        print("Success!")
    except Exception:
        print("SHAP Failed!")
        traceback.print_exc()

if __name__ == "__main__":
    debug_shap()
