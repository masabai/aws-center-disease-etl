"""
Soda Core validation for CDC datasets.
Alternative to Great Expectations (phase1-pandas-dag/etl/validate_cdc.py).
Uses YAML-driven SodaCL checks via soda-core-pandas-dask 3.3.22.
"""
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from soda.scan import Scan

# Base directories for processed data, Soda check configs, and validation outputs

BASE_DIR      = Path("/opt/airflow/data")
PROCESSED_DIR = BASE_DIR / "processed"
CHECKS_DIR    = Path("/opt/airflow/checks")
OUTPUT_DIR    = BASE_DIR / "validation"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cleaned CDC datasets paired with their SodaCL check files
DATASETS = [
    ("chronic_cleaned.csv", "chronic_checks.yml", "chronic_cleaned"),
    ("heart_cleaned.csv",   "heart_checks.yml",   "heart_cleaned"),
    ("nutri_cleaned.csv",   "nutri_checks.yml",   "nutri_cleaned"),
]


def run_soda_scan(df, checks_file, dataset_name):
    """
    Run a Soda Core scan against a single pandas DataFrame.

    Registers the DataFrame as a Dask-backed pandas data source,
    loads SodaCL checks from YAML, executes the scan, and returns
    the Scan object for result inspection.
    """
    scan = Scan()
    scan.set_scan_definition_name(dataset_name)
    scan.set_data_source_name("pandas")

    # Register DataFrame and attach YAML check definitions
    scan.add_pandas_dataframe(data_source_name="pandas", dataset_name=dataset_name, pandas_df=df)
    scan.add_sodacl_yaml_file(str(checks_file))
    scan.execute()
    return scan


def validate_all_csvs():
    """
    Airflow task wrapper to validate all processed CDC CSV files.

    Iterates through cleaned datasets, runs Soda Core scans,
    and persists JSON validation reports to the validation/ directory.
    Raises ValueError if any dataset fails, failing the Airflow task.
    """
    all_passed = True

    for csv_file, checks_file, dataset_name in DATASETS:
        csv_path    = PROCESSED_DIR / csv_file
        checks_path = CHECKS_DIR / checks_file

        print(f"\nValidating {csv_file} with {checks_file}")
        df   = pd.read_csv(csv_path, low_memory=False)
        scan = run_soda_scan(df, checks_path, dataset_name)

        # Persist scan summary and pass/fail status to disk
        output_file = OUTPUT_DIR / f"soda_{dataset_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        result = {
            "dataset": dataset_name,
            "passed":  not scan.has_check_fails(),
            "logs":    scan.get_logs_text(),
        }
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)

        print(scan.get_logs_text())
        print(f"Results saved: {output_file}")

        if scan.has_check_fails():
            all_passed = False
            print(f"FAILED: {csv_file}")

    if not all_passed:
        raise ValueError("Soda validation failed — check validation/ folder for details")
