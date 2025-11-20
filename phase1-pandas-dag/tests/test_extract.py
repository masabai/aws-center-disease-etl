import os
import urllib.request
import pytest
import ssl


ssl._create_default_https_context = ssl._create_unverified_context

# URLs for CSVs
urls = [
    "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
    "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD"
]

# Local folder for testing
local_folder = "./data/raw/"
os.makedirs(local_folder, exist_ok=True)
local_files = [os.path.join(local_folder, f) for f in ["Chronic_Disease.csv", "Heart_Disease.csv", "Nutrition.csv"]]

@pytest.mark.parametrize("url,local_file", zip(urls, local_files))
def test_extract(url, local_file):
    """Download CSV locally and check it exists and is not empty."""
    urllib.request.urlretrieve(url, local_file)
    assert os.path.exists(local_file), f"{local_file} not found"
    assert os.path.getsize(local_file) > 0, f"{local_file} is empty"
