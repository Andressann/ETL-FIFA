import pandas as pd


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print("TRANSFORM iniciado")
    print("Columnas originales:", df.columns)

    # Limpieza básica
    df = df.copy()
    df.drop_duplicates(inplace=True)

    # Normalizar textos
    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"": None, "nan": None, "None": None})

    print("Filas después de limpiar:", df.shape)
    return df