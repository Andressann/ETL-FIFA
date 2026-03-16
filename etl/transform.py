def transform_data(df):

    print("TRANSFORM iniciado")

    print("Columnas originales:", df.columns)

    # ejemplo simple
    df = df.drop_duplicates()

    print("Filas después de limpiar:", df.shape)

    return df