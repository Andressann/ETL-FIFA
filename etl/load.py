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


def build_engine(database):
    url = URL.create(
        drivername="postgresql+pg8000",
        username=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=database,
    )

    return create_engine(url, pool_pre_ping=True)


def ensure_database_exists():

    engine = build_engine("postgres")

    with engine.connect() as conn:

        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :db"),
            {"db": DB_CONFIG["database"]},
        ).scalar()

        if not exists:

            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE {DB_CONFIG['database']}")
            )

            print("Base de datos creada")

    engine.dispose()


def load_data(df):

    print("LOAD iniciado")

    if df.shape[1] == 1 and ";" in str(df.columns[0]):
        raise ValueError("CSV mal leído. Usa sep=';' en extract.py.")

    ensure_database_exists()

    engine = build_engine(DB_CONFIG["database"])

    df.to_sql(
        TABLE_NAME,
        engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000,
    )

    engine.dispose()

    print("Datos cargados correctamente")