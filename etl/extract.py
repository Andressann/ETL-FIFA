import os
from pathlib import Path

import pandas as pd

DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "FullData.csv"


def extract_data():
    print("EXTRACT iniciado")
    path = os.getenv("DATA_CSV_PATH", str(DEFAULT_CSV_PATH))
    df = pd.read_csv(path, sep=";")
    print("Datos extraídos:", df.shape)
    return df