from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

DB_CONFIG = {
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": 5432,
    "database": "fifa",
}

TABLE_NAME = "players"


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def build_engine(database: str, autocommit: bool = False):
    url = URL.create(
        drivername="postgresql+pg8000",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=database,
    )
    if autocommit:
        return create_engine(url, pool_pre_ping=True, isolation_level="AUTOCOMMIT")
    return create_engine(url, pool_pre_ping=True)


def ensure_database_exists():
    admin_engine = build_engine("postgres", autocommit=True)
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": DB_CONFIG["database"]},
            ).scalar()

            if not exists:
                conn.exec_driver_sql(f"CREATE DATABASE {_quote_ident(DB_CONFIG['database'])}")
                print(f"Base de datos creada: {DB_CONFIG['database']}")
    finally:
        admin_engine.dispose()


def _safe_chunksize(n_cols: int, max_params: int = 60000, max_rows: int = 1000) -> int:
    n_cols = max(1, int(n_cols))
    return max(1, min(max_rows, max_params // n_cols))


def load_data(df):
    print("LOAD iniciado")

    if df.shape[1] == 1 and ";" in str(df.columns[0]):
        raise ValueError("CSV mal leído. Usa sep=';' en extract.py.")

    ensure_database_exists()

    engine = build_engine(DB_CONFIG["database"])
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")

        safe_chunk = _safe_chunksize(df.shape[1])
        print(f"Insertando con chunksize seguro: {safe_chunk}")

        try:
            # Más rápido: inserción multi-row con lote seguro
            df.to_sql(
                TABLE_NAME,
                engine,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=safe_chunk,
            )
        except Exception as e:
            # Fallback para límite de parámetros en pg8000
            if "'H' format requires" in str(e):
                print("Fallback: inserción por lotes sin method='multi'")
                df.to_sql(
                    TABLE_NAME,
                    engine,
                    if_exists="replace",
                    index=False,
                    chunksize=1000,
                )
            else:
                raise

        print("Datos cargados")
    finally:
        engine.dispose()