import os
import urllib.request
import pytest
import ssl

# Disable SSL verification for test downloads (needed in some CI environments)
ssl._create_default_https_context = ssl._create_unverified_context

# CDC CSV download URLs used for extraction tests
urls = [
    "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD"
]

# Local directory for storing downloaded test files
local_folder = "./data/raw/"
os.makedirs(local_folder, exist_ok=True)

# Local file paths corresponding to each CDC dataset
local_files = [
    os.path.join(local_folder, f)
    for f in ["Chronic_Disease.csv", "Heart_Disease.csv", "Nutrition.csv"]
]


@pytest.mark.parametrize("url,local_file", zip(urls, local_files))
def test_extract(url, local_file):
    """
    Verify CDC CSV files can be downloaded successfully.

    Downloads each dataset to a local path and asserts that:
    - the file exists
    - the file is not empty
    """
    # Download CSV file
    urllib.request.urlretrieve(url, local_file)

    # Validate file creation and size
    assert os.path.exists(local_file), f"{local_file} not found"
    assert os.path.getsize(local_file) > 0, f"{local_file} is empty"
