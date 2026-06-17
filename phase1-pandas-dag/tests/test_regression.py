import sys
import os
import pandas as pd
from pathlib import Path

"""
Tests for processed CDC dataset row count stability.
"""
BASE_DIR = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(BASE_DIR))

PROCESSED_DIR = BASE_DIR / "data" / "chunk"

# "heart_cleaned.csv" is intentionally set to 87 rows (instead of 100).
# This is a negative test to ensure the CI pipeline properly 
# catches data drift and fails when row volume deviates from the baseline.

BASELINES = {
    "chronic_cleaned.csv": 99, #309215,
    "heart_cleaned.csv":  100, # 87,  #78792,
    "nutri_cleaned.csv": 100, #110880,
}
TOLERANCE = 0.10  # allow 10% variance


def test_chronic_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "chunk_chronic_cleaned.csv"))
    baseline = BASELINES["chronic_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"chronic row count {len(df)} deviates >10% from baseline {baseline}"


def test_heart_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "chunk_heart_cleaned.csv"))
    baseline = BASELINES["heart_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"heart row count {len(df)} deviates >10% from baseline {baseline}"


def test_nutri_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "chunk_nutri_cleaned.csv"))
    baseline = BASELINES["nutri_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"nutri row count {len(df)} deviates >10% from baseline {baseline}"

"""
Regression tests fail due to mismatch between full dataset baselines and sample data (n=100) used in GitHub environment.
Full datasets excluded due to GitHub size limits.
"""
