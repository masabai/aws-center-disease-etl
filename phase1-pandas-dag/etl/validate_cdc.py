import glob
import os
from pathlib import Path
import pandas as pd
import json
from datetime import datetime
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectTableColumnCountToEqual,
    ExpectColumnToExist,
    ExpectColumnValuesToMatchRegex,
    ExpectColumnValuesToBeInSet
)

# Base directories for processed data and validation outputs
BASE_DIR = Path("/opt/airflow/data")

PROCESSED_DIR = BASE_DIR / "processed"
GX_OUTPUT_DIR = BASE_DIR / "validation"
GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Cleaned CDC datasets to validate
CSV_FILES = ["chronic_cleaned.csv", "heart_cleaned.csv", "nutri_cleaned.csv"]


def run_data_validation(df, batch_name="my_batch"):
    """
    Run Great Expectations validation against a single pandas DataFrame.

    Defines table-level and column-level expectations dynamically,
    executes validation, and returns the validation result object.
    """
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name=batch_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    context.suites.add(suite)

    # Table-level expectations
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=50000, max_value=200000))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df.columns) + 1)) # A new column added to original DF

    # Required column checks
    expected_columns = [
        "year_start", "location_abbr", "data_value",
        "low_confidence_limit", "high_confidence_limit",
        "stratification_value"
    ]
    for col in expected_columns:
        suite.add_expectation(ExpectColumnToExist(column=col))

    # Value range validation for year column
    if "year_start" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(
                column="year_start", min_value=2011, max_value=2024
            )
        )

    # Location code validation
    if "location_abbr" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeInSet(
                column="location_abbr",
                value_set=[
                    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
                    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
                    "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
                    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
                    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "US", "PR", "GU",
                    "AI", "VI", "MP"
                ]
            )
        )
        suite.add_expectation(
            ExpectColumnValuesToMatchRegex(column="location_abbr", regex="^[A-Z]{2}$")
        )

    # Metric value bounds
    if "data_value" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(
                column="data_value", min_value=0, max_value=86
            )
        )

    # Confidence interval consistency check
    if {"low_confidence_limit", "high_confidence_limit"}.issubset(df.columns):
        df["low_le_high"] = df["low_confidence_limit"] <= df["high_confidence_limit"]
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(column="low_le_high", value_set=[True])
        )

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )
    results = validation_definition.run(batch_parameters={"dataframe": df})
    return results


def validate_all_csvs():
    """
    Airflow task wrapper to validate all processed CDC CSV files.

    Iterates through cleaned datasets, runs Great Expectations validation,
    and persists JSON validation reports locally.
    """
    for file_name in CSV_FILES:
        file_path = PROCESSED_DIR / file_name
        batch_name = file_name.split(".")[0]

        print(f"\nValidating {file_path}")
        df = pd.read_csv(file_path)
        validation_results = run_data_validation(df, batch_name=batch_name)

        # Persist validation results to disk
        output_file = GX_OUTPUT_DIR / f"gx_{batch_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(validation_results.to_json_dict(), f, indent=2)

        print(f"Validation results saved: {output_file}")


def _extract_row_count(gx_json):
    """Extract observed row count from a GX JSON result dict."""
    for r in gx_json.get("results", []):
        if r.get("expectation_config", {}).get("type") == "expect_table_row_count_to_be_between":
            return r.get("result", {}).get("observed_value")
    return None


def _extract_null_rate(gx_json, column="data_value"):
    """Extract null rate (0–1) for a given column from a GX JSON result dict."""
    for r in gx_json.get("results", []):
        cfg = r.get("expectation_config", {})
        if (
            cfg.get("type") == "expect_column_values_to_not_be_null"
            and cfg.get("kwargs", {}).get("column") == column
        ):
            unexpected_pct = r.get("result", {}).get("unexpected_percent", 0)
            return unexpected_pct / 100
    return None


def check_observability_drift():
    """
    Airflow task: compare current GX run against previous run per dataset.

    Raises ValueError (fails the task → triggers notify_slack_fail) if:
    - Volume drops >10% vs previous run  (volume anomaly)
    - Null rate on data_value rises >5pp vs previous run  (null rate spike)
    """
    VOLUME_DROP_THRESHOLD = 0.10   # 10% row count drop
    NULL_RATE_SPIKE_THRESHOLD = 0.05  # 5 percentage-point rise

    datasets = [f.split(".")[0] for f in CSV_FILES]  # e.g. "chronic_cleaned"
    anomalies = []

    for dataset in datasets:
        pattern = str(GX_OUTPUT_DIR / f"gx_{dataset}_*.json")
        files = sorted(glob.glob(pattern))

        if len(files) < 2:
            print(f"[observability] {dataset}: only {len(files)} run(s) — skipping drift check")
            continue

        with open(files[-2]) as f:
            prev = json.load(f)
        with open(files[-1]) as f:
            curr = json.load(f)

        # --- Volume anomaly ---
        prev_rows = _extract_row_count(prev)
        curr_rows = _extract_row_count(curr)
        if prev_rows and curr_rows:
            drop_pct = (prev_rows - curr_rows) / prev_rows
            print(f"[observability] {dataset} rows: {prev_rows} → {curr_rows} ({drop_pct:+.1%})")
            if drop_pct > VOLUME_DROP_THRESHOLD:
                anomalies.append(
                    f"Volume anomaly [{dataset}]: {drop_pct:.1%} drop ({prev_rows} → {curr_rows})"
                )

        # --- Null rate spike ---
        prev_null = _extract_null_rate(prev)
        curr_null = _extract_null_rate(curr)
        if prev_null is not None and curr_null is not None:
            spike = curr_null - prev_null
            print(f"[observability] {dataset} null rate: {prev_null:.1%} → {curr_null:.1%} ({spike:+.1%})")
            if spike > NULL_RATE_SPIKE_THRESHOLD:
                anomalies.append(
                    f"Null rate spike [{dataset}]: {prev_null:.1%} → {curr_null:.1%} (+{spike:.1%})"
                )

    if anomalies:
        raise ValueError("Observability drift detected:\n" + "\n".join(anomalies))
