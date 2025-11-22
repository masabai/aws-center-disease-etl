import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.simplefilter("ignore", InsecureRequestWarning)

import requests
import pandas as pd
from io import StringIO
from pathlib import Path
import os

def extract():
    """
    Downloads CDC CSVs and saves to raw directory.
    Works both locally (Windows) and inside Docker.
    """
    urls = [
        "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
        "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
        "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD"
    ]
    local_files = ['chronic.csv', 'heart.csv', 'nutri.csv']

    # OS-aware paths
    if os.name == "nt":  # Windows
        BASE_DIR = Path(__file__).parent.parent / "data"
    else:  # Linux/Docker
        BASE_DIR = Path("/opt/airflow/data")

    RAW_DIR = BASE_DIR / "raw"
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for url, local_file in zip(urls, local_files):
        response = requests.get(url, verify=False)
        df = pd.read_csv(StringIO(response.text))
        csv_path = RAW_DIR / local_file
        df.to_csv(csv_path, index=False)
        print(f"Saved: {csv_path}")
