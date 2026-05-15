import sys
import os
import pandas as pd

"""
Tests for processed CDC dataset row count stability.
"""
BASE_DIR = Path(__file__).resolve().parent.parent

sys.path.insert(0, str(BASE_DIR))

PROCESSED_DIR = BASE_DIR / "data" / "processed"

# Baselines: row counts from last known good pipeline run
BASELINES = {
    "chronic_cleaned.csv": 309215,
    "heart_cleaned.csv": 78792,
    "nutri_cleaned.csv": 110880,
}
TOLERANCE = 0.10  # allow 10% variance


def test_chronic_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "chronic_cleaned.csv"))
    baseline = BASELINES["chronic_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"chronic row count {len(df)} deviates >10% from baseline {baseline}"


def test_heart_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "heart_cleaned.csv"))
    baseline = BASELINES["heart_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"heart row count {len(df)} deviates >10% from baseline {baseline}"


def test_nutri_row_count_stable():
    df = pd.read_csv(os.path.join(PROCESSED_DIR, "nutri_cleaned.csv"))
    baseline = BASELINES["nutri_cleaned.csv"]
    assert abs(len(df) - baseline) / baseline < TOLERANCE, \
        f"nutri row count {len(df)} deviates >10% from baseline {baseline}"
