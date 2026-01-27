import requests
import pandas as pd
from io import StringIO
from pathlib import Path


def extract():
    """
    Download CDC public datasets as CSV files and store them in the raw data directory.

    This function fetches multiple CDC datasets via HTTPS, loads them into pandas
    DataFrames, and persists them as CSV files under `/opt/airflow/data/raw`.
    Designed to run both locally and inside an Airflow Docker container.
    """
    urls = [
        "https://data.cdc.gov/api/views/hksd-2xuw/rows.csv?accessType=DOWNLOAD",
        "https://data.cdc.gov/api/views/55yu-xksw/rows.csv?accessType=DOWNLOAD",
        "https://data.cdc.gov/api/views/hn4x-zwk7/rows.csv?accessType=DOWNLOAD",
    ]
    local_files = ["chronic.csv", "heart.csv", "nutri.csv"]

    base_dir = Path("/opt/airflow/data")
    raw_dir = base_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for url, local_file in zip(urls, local_files):
        response = requests.get(url)
        response.raise_for_status()  # fail fast on network or API errors

        df = pd.read_csv(StringIO(response.text))
        csv_path = raw_dir / local_file
        df.to_csv(csv_path, index=False)

        print(f"Saved: {csv_path}")
