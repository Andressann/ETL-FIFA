import os
import tempfile
import pandas as pd
from pathlib import Path

DEFAULT_CSV_PATH = Path(__file__).parent.parent / "data" / "raw" / "FullData.csv"

def extract_data() -> pd.DataFrame:
    if os.getenv("S3_BUCKET"):
        from etl.s3_utils import download_csv_from_s3
        local_path = os.path.join(tempfile.gettempdir(), "FullData.csv")
        path = download_csv_from_s3(local_path)
    else:
        path = os.getenv("DATA_CSV_PATH", str(DEFAULT_CSV_PATH))
    return pd.read_csv(path, sep=";")
