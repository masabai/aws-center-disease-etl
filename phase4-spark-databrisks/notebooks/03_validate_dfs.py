# Databricks notebook source
# MAGIC %pip install great_expectations --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run "./02_transform"

# COMMAND ----------

import great_expectations as gx
from great_expectations.expectations import ExpectTableColumnCountToEqual, ExpectColumnToExist,  ExpectColumnValuesToNotBeNull, ExpectColumnValuesToBeBetween, ExpectColumnValuesToNotBeInSet
import pandas as pd


# Ephemeral GE context 
context = gx.get_context()

# Validate a pandas DataFrame with expectations, returns a list of results per expectation and overall success.   
def validate_pandas_df(df_pd, dataset_name):
  
    suite_name = f"suite_{dataset_name}"
    
    # Create Expectation Suite ---
    suite = gx.ExpectationSuite(name=suite_name)
    
    # Add expectations
    suite.add_expectation(ExpectColumnToExist(column="Topic"))
    suite.add_expectation(ExpectColumnToExist(column='temperature'))
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df_pd.columns)))
    suite.add_expectation(ExpectColumnValuesToNotBeNull(column="Topic"))

    # Register suite
    context.suites.add(suite)
    
    # Pandas datasource & asset 
    datasource = context.data_sources.add_or_update_pandas(name=f"datasource_{dataset_name}")
    asset = datasource.add_dataframe_asset(name=f"asset_{dataset_name}")
    batch_request = asset.build_batch_request(options={"dataframe": df_pd})

    # Validator 
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    result = validator.validate()
    
    # Collect results with actual expectation names ---
    summary = []
    for r in result["results"]:
        exp_conf = r.get("expectation_config", {})
        expectation_name = exp_conf.get("type", "Unknown")
        
        summary.append({
            "dataset": dataset_name,
            "expectation": expectation_name,
            "success": r.get("success", False)
        })
    
    return summary, result["success"]

# Convert Spark DataFrames to Pandas ---

dfs_pd = {
    "dataset1": df_chronic.toPandas(),   
    "dataset2": df_heart.toPandas(),    
    "dataset3": df_nutri.toPandas()      
}


# Run validations and compute overall pass per dataset ---
all_results = []
overall_pass_dict = {}

for name, df_pd in dfs_pd.items():
    summary, overall = validate_pandas_df(df_pd, name)
    all_results.extend(summary)
    overall_pass_dict[name] = overall  # True only if all expectations passed

# Convert to DataFrame ---
summary_df = pd.DataFrame(all_results)

# Add overall dataset pass/fail column ---
summary_df["overall_pass"] = summary_df["dataset"].map(overall_pass_dict)


# Display summary table ---
display(summary_df)

# COMMAND ----------

# ===============================
# 03_validate_dfs.ipynb
# CDC ETL Data Validation (Free Edition / Databricks)
# ===============================

import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableColumnCountToEqual, 
    ExpectTableRowCountToBeBetween,
    ExpectColumnToExist,  
    ExpectColumnValuesToNotBeNull,
    ExpectColumnValuesToBeBetween,
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectTableColumnsToMatchSet,
    ExpectColumnValuesToMatchRegex
)
import pandas as pd

# --- Ephemeral GE context
context = gx.get_context()

# ----------------------------
# Dataset-specific configuration
# ----------------------------
dataset_configs = {
    "chronic": {
        "df": df_chronic.toPandas(),  # small dataset only
        "expected_columns": ["YEARSTART","YEAREND","LOCATIONABBR","LOCATIONDESC","DATASOURCE",
                             "TOPIC","QUESTION","RESPONSE","DATAVALUEUNIT","DATAVALUETYPE",
                             "DATAVALUE","DATAVALUEALT","DATAVALUEFOOTNOTESYMBOL","DATAVALUEFOOTNOTE",
                             "LOWCONFIDENCELIMIT","HIGHCONFIDENCELIMIT","STRATIFICATIONCATEGORY1",
                             "STRATIFICATION1","STRATIFICATIONCATEGORY2","STRATIFICATION2","STRATIFICATIONCATEGORY3",
                             "STRATIFICATION3","GEOLOCATION","LOCATIONID","TOPICID","QUESTIONID","RESPONSEID",
                             "DATAVALUETYPEID","STRATIFICATIONCATEGORYID1","STRATIFICATIONID1","STRATIFICATIONCATEGORYID2",
                             "STRATIFICATIONID2","STRATIFICATIONCATEGORYID3","STRATIFICATIONID3"],
        "numeric_columns": ["DATAVALUE","LOWCONFIDENCELIMIT","HIGHCONFIDENCELIMIT"],
        "categorical_columns": {"TOPIC": None},  # can fill allowed sets if known
        "unique_columns": ["QUESTIONID","RESPONSEID"],
        "row_count_range": (300000, 310000)
    },
    "heart": {
        "df": df_heart.toPandas(),
        "expected_columns": ["YEAR","LOCATIONABBR","LOCATIONDESC","GEOGRAPHICLEVEL","DATASOURCE",
                             "CLASS","TOPIC","DATA_VALUE","DATA_VALUE_UNIT","DATA_VALUE_TYPE",
                             "DATA_VALUE_FOOTNOTE_SYMBOL","DATA_VALUE_FOOTNOTE","STRATIFICATIONCATEGORY1",
                             "STRATIFICATION1","STRATIFICATIONCATEGORY2","STRATIFICATION2",
                             "TOPICID","LOCATIONID","Y_LAT","X_LON","GEOREFERENCE"],
        "numeric_columns": ["DATA_VALUE"],
        "categorical_columns": {"CLASS": None,"TOPIC": None},
        "unique_columns": ["TOPICID","LOCATIONID"],
        "row_count_range": (75000, 80000)
    },
    "nutri": {
        "df": df_nutri.toPandas(),
        "expected_columns": ["YEARSTART","YEAREND","LOCATIONABBR","LOCATIONDESC","DATASOURCE","CLASS",
                             "TOPIC","QUESTION","DATA_VALUE_UNIT","DATA_VALUE_TYPE","DATA_VALUE","DATA_VALUE_ALT",
                             "DATA_VALUE_FOOTNOTE_SYMBOL","DATA_VALUE_FOOTNOTE","LOW_CONFIDENCE_LIMIT","HIGH_CONFIDENCE_LIMIT",
                             "SAMPLE_SIZE","TOTAL","AGE","EDUCATION","SEX","INCOME","RACE_ETHNICITY","GEOLOCATION",
                             "CLASSID","TOPICID","QUESTIONID","DATAVALUETYPEID","LOCATIONID","STRATIFICATIONCATEGORY1",
                             "STRATIFICATION1","STRATIFICATIONCATEGORYID1","STRATIFICATIONID1"],
        "numeric_columns": ["DATA_VALUE","LOW_CONFIDENCE_LIMIT","HIGH_CONFIDENCE_LIMIT","SAMPLE_SIZE","TOTAL"],
        "categorical_columns": {"CLASS": None,"TOPIC": None},
        "unique_columns": ["QUESTIONID","TOPICID","LOCATIONID"],
        "row_count_range": (100000, 110000)
    }
}

# ----------------------------
# Validation function
# ----------------------------
def validate_dataset(dataset_name, config):
    df_pd = config["df"]
    suite_name = f"suite_{dataset_name}"
    suite = gx.ExpectationSuite(name=suite_name)
    
    # 1️⃣ Column existence & non-null
    for col in config["expected_columns"]:
        if col in df_pd.columns:
            suite.add_expectation(ExpectColumnToExist(column=col))
            suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col))
        else:
            print(f"[WARNING] Column '{col}' not in {dataset_name}, skipping existence/null expectations")
    
    # 2️⃣ Row count
    min_row, max_row = config["row_count_range"]
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=min_row, max_value=max_row))
    
    # 3️⃣ Numeric ranges
    for col in config.get("numeric_columns", []):
        if col in df_pd.columns:
            min_val, max_val = df_pd[col].min(), df_pd[col].max()
            suite.add_expectation(ExpectColumnValuesToBeBetween(column=col, min_value=min_val, max_value=max_val))
    
    # 4️⃣ Categorical sets
    for col, allowed_set in config.get("categorical_columns", {}).items():
        if col in df_pd.columns and allowed_set:
            suite.add_expectation(ExpectColumnValuesToBeInSet(column=col, value_set=allowed_set))
    
    # 5️⃣ Unique columns
    for col in config.get("unique_columns", []):
        if col in df_pd.columns:
            suite.add_expectation(ExpectColumnValuesToBeUnique(column=col))
    
    # 6️⃣ Column count
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(df_pd.columns)))
    
    # Register suite
    context.suites.add(suite)
    
    # --- Pandas datasource & validator ---
    datasource = context.data_sources.add_or_update_pandas(name=f"datasource_{dataset_name}")
    asset = datasource.add_dataframe_asset(name=f"asset_{dataset_name}")
    batch_request = asset.build_batch_request(options={"dataframe": df_pd})
    validator = context.get_validator(batch_request=batch_request, expectation_suite_name=suite_name)
    result = validator.validate()
    
    # Collect results
    summary = []
    for r in result["results"]:
        exp_conf = r.get("expectation_config", {})
        expectation_name = exp_conf.get("type", "Unknown")
        summary.append({
            "dataset": dataset_name,
            "expectation": expectation_name,
            "success": r.get("success", False)
        })
    
    overall_success = result["success"]
    return summary, overall_success

# ----------------------------
# Run validations
# ----------------------------
all_results = []
overall_pass_dict = {}

for name, config in dataset_configs.items():
    summary, overall = validate_dataset(name, config)
    all_results.extend(summary)
    overall_pass_dict[name] = overall

# ----------------------------
# Summary DataFrame
# ----------------------------
summary_df = pd.DataFrame(all_results)
summary_df["overall_pass"] = summary_df["dataset"].map(overall_pass_dict)

# Display
display(summary_df)
