import pandas as pd


def extract_data():
    print("EXTRACT iniciado")
    path = "data/raw/FullData.csv"
    df = pd.read_csv(path, sep=";")
    print("Datos extraídos:", df.shape)
    return df