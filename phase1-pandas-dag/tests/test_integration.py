import sys
import os
import pandas as pd
import pytest

"""
Tests for CDC ETL transform logic.

Validates cleaned output dataframe structure and data quality rules.
"""

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from etl.transform_cdc import clean_dataframe

RAW_DIR = os.path.join(os.path.dirname(__file__), "data", "raw")


@pytest.fixture
def chronic_df():
    return pd.read_csv(os.path.join(RAW_DIR, "chronic.csv")) # "Chronic_Disease.csv"))


@pytest.fixture
def heart_df():
    return pd.read_csv(os.path.join(RAW_DIR, "heart.csv")) #Heart_Disease.csv"))


@pytest.fixture
def nutri_df():
    return pd.read_csv(os.path.join(RAW_DIR, "nutri.csv")) #"Nutrition.csv"))


def assert_pipeline_output(raw_df, cleaned_df):
    assert len(cleaned_df) > 0, "Pipeline produced empty DataFrame"
    assert len(cleaned_df) <= len(raw_df), "Cleaned rows exceed raw rows"

    for col in cleaned_df.columns:
        assert col == col.lower(), f"Column not snake_case: {col}"
        assert " " not in col, f"Column has spaces: {col}"

    assert "data_value" in cleaned_df.columns
    assert "location_abbr" in cleaned_df.columns
    assert "year_start" in cleaned_df.columns or "year" in cleaned_df.columns

    year_col = "year_start" if "year_start" in cleaned_df.columns else "year"
    assert cleaned_df["data_value"].dtype.name == "float32"
    assert str(cleaned_df[year_col].dtype) in ("Int64", "int64", "float64")

    assert cleaned_df["data_value"].isnull().sum() == 0
    assert cleaned_df["location_abbr"].isnull().sum() == 0


def test_chronic_pipeline(chronic_df):
    cleaned = clean_dataframe(chronic_df)
    assert_pipeline_output(chronic_df, cleaned)


def test_heart_pipeline(heart_df):
    cleaned = clean_dataframe(heart_df)
    assert_pipeline_output(heart_df, cleaned)


def test_nutri_pipeline(nutri_df):
    cleaned = clean_dataframe(nutri_df)
    assert_pipeline_output(nutri_df, cleaned)
