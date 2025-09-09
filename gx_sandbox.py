import great_expectations as gx
import pandas as pd
from pathlib import Path

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
filepath = DATA_DIR / "Heart_Disease_Mortality_Data_Among_US_Adults__35___by_State_Territory_and_County___2019-2021.csv"

# Read CSV
dataframe = pd.read_csv(filepath)

# Create Ephemeral Context
context = gx.get_context()

# Add Pandas Datasource
datasource = context.data_sources.add_pandas(name="my_pandas_datasource")

# Add Data Asset
data_asset = datasource.add_dataframe_asset(name="my_dataframe_asset")

# Create Batch Definition
batch_definition = data_asset.add_batch_definition_whole_dataframe(name="my_batch_definition")

# Get a Batch
batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

# Retrieve Validator (no suite needed)
validator = context.get_validator(batch=batch)

# Inspect columns
print("Columns:", validator.columns)

# Add expectation
validator.expect_table_column_count_to_equal(value=21)

# Validate
validation_result = validator.validate()
print(validation_result.success)

