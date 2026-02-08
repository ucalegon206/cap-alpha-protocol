import pytest
import sys
import os

if __name__ == "__main__":
    # Ensure libs and root are in path
    root = os.getcwd()
    libs = os.path.join(root, "libs")
    sys.path.insert(0, root)
    sys.path.insert(0, libs)
    
    # Target specific test files to avoid scanning the root
    test_files = [
        "tests/test_feature_store.py",
        "tests/test_strategic_engine.py",
        "tests/test_data_quality.py",
        "tests/test_gold_integrity.py",
        "tests/test_dag_integrity.py"
    ]
    
    # Run pytest directly on these files
    # -c /dev/null ensures it doesn't try to read the root pytest.ini or scan parents
    args = [
        "-v",
        "-c", "/dev/null",
        "--rootdir", os.path.join(root, "tests")
    ] + test_files
    
    print(f"Running pytest with args: {args}")
    sys.exit(pytest.main(args))
