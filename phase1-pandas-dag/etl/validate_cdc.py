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

if os.name == "nt":
    BASE_DIR = Path("C:/PythonProject/CenterDiseaseControl/phase1-pandas-dag/data")
else:
    BASE_DIR = Path("/opt/airflow/data")

PROCESSED_DIR = BASE_DIR / "processed"
GX_OUTPUT_DIR = BASE_DIR / "validation"
GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_FILES = ["chronic_cleaned.csv", "heart_cleaned.csv", "nutri_cleaned.csv"]

def run_data_validation(df, batch_name="my_batch"):
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name=batch_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)

    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    context.suites.add(suite)

    # Table expectations
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=50000, max_value=200000))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df.columns) + 1))

    # Column expectations
    expected_columns = [
        "year_start", "location_abbr", "data_value",
        "low_confidence_limit", "high_confidence_limit",
        "stratification_value"
    ]
    for col in expected_columns:
        suite.add_expectation(ExpectColumnToExist(column=col))

    if "year_start" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(column="year_start", min_value=2011, max_value=2023)
        )
    if "location_abbr" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeInSet(
                column="location_abbr",
                value_set=[ "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA",
                            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA",
                            "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
                            "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
                            "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "US", "PR", "GU",
                            "AI", "VI", "MP"
                          ]
            )
        )
        suite.add_expectation(ExpectColumnValuesToMatchRegex(column="location_abbr", regex="^[A-Z]{2}$"))

    if "data_value" in df.columns:
        suite.add_expectation(
            gx.expectations.core.ExpectColumnValuesToBeBetween(column="data_value", min_value=0, max_value=86)
        )

    if {"low_confidence_limit", "high_confidence_limit"}.issubset(df.columns):
        df["low_le_high"] = df["low_confidence_limit"] <= df["high_confidence_limit"]
        suite.add_expectation(ExpectColumnValuesToBeInSet(column="low_le_high", value_set=[True]))

    validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=f"validation_{batch_name}")
    results = validation_definition.run(batch_parameters={"dataframe": df})
    return results

# DAG wrapper
def validate_all_csvs():
    for file_name in CSV_FILES:
        file_path = PROCESSED_DIR / file_name
        batch_name = file_name.split(".")[0]
        print(f"\nValidating {file_path}")
        df = pd.read_csv(file_path)
        validation_results = run_data_validation(df, batch_name=batch_name)

        # Save results locally
        output_file = GX_OUTPUT_DIR / f"gx_{batch_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(validation_results.to_json_dict(), f, indent=2)
        print(f"Validation results saved: {output_file}")
