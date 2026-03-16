import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

st.set_page_config(page_title="FIFA ETL - Dashboard", layout="wide")
st.title("⚽ FIFA ETL - Dashboard de validación")

DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "fifa"
TABLE_NAME = "players"


@st.cache_resource
def get_engine():
    url = URL.create(
        drivername="postgresql+pg8000",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )
    return create_engine(url, pool_pre_ping=True)


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


@st.cache_data(ttl=60)
def load_summary():
    sql = f"""
    WITH base AS (
      SELECT
        "Name",
        "Club",
        "Nationality",
        CASE
          WHEN TRIM("Rating"::text) ~ '^[0-9]+$' THEN "Rating"::int
          ELSE NULL
        END AS rating
      FROM "{TABLE_NAME}"
    )
    SELECT
      COUNT(*)::int AS total_rows,
      COUNT(rating)::int AS rating_not_null,
      COALESCE(MAX(rating), 0)::int AS max_rating,
      COALESCE(AVG(rating), 0)::numeric(10,2) AS avg_rating
    FROM base;
    """
    return run_query(sql)


@st.cache_data(ttl=60)
def load_top_players(limit: int):
    sql = f"""
    SELECT
      "Name",
      "Club",
      "Nationality",
      CASE
        WHEN TRIM("Rating"::text) ~ '^[0-9]+$' THEN "Rating"::int
        ELSE NULL
      END AS rating
    FROM "{TABLE_NAME}"
    WHERE TRIM("Rating"::text) ~ '^[0-9]+$'
    ORDER BY rating DESC, "Name" ASC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})


@st.cache_data(ttl=60)
def load_top_nationalities(limit: int = 15):
    sql = f"""
    SELECT
      "Nationality",
      COUNT(*)::int AS total
    FROM "{TABLE_NAME}"
    WHERE "Nationality" IS NOT NULL
    GROUP BY "Nationality"
    ORDER BY total DESC, "Nationality" ASC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})


@st.cache_data(ttl=60)
def load_nationalities():
    sql = f"""
    SELECT DISTINCT "Nationality"
    FROM "{TABLE_NAME}"
    WHERE "Nationality" IS NOT NULL
    ORDER BY 1;
    """
    return run_query(sql)


@st.cache_data(ttl=60)
def load_players_by_nationality(nat: str, limit: int):
    sql = f"""
    SELECT
      "Name",
      "Club",
      CASE
        WHEN TRIM("Rating"::text) ~ '^[0-9]+$' THEN "Rating"::int
        ELSE NULL
      END AS rating
    FROM "{TABLE_NAME}"
    WHERE "Nationality" = :nat
    ORDER BY rating DESC NULLS LAST, "Name" ASC
    LIMIT :limit;
    """
    return run_query(sql, {"nat": nat, "limit": limit})


try:
    summary = load_summary().iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total filas", int(summary["total_rows"]))
    c2.metric("Ratings válidos", int(summary["rating_not_null"]))
    c3.metric("Rating máximo", int(summary["max_rating"]))
    c4.metric("Rating promedio", float(summary["avg_rating"]))

    st.divider()

    left, right = st.columns([2, 1])

    with left:
        st.subheader("🏆 Top jugadores por rating")
        top_n = st.slider("Cantidad de jugadores", min_value=5, max_value=50, value=10, step=5)
        df_top = load_top_players(top_n)
        st.dataframe(df_top, use_container_width=True, hide_index=True)

        chart_df = df_top[["Name", "rating"]].set_index("Name")
        st.bar_chart(chart_df)

    with right:
        st.subheader("🌍 Top nacionalidades")
        df_nats = load_top_nationalities(15)
        st.dataframe(df_nats, use_container_width=True, hide_index=True)
        st.bar_chart(df_nats.set_index("Nationality"))

    st.divider()
    st.subheader("🔎 Jugadores por nacionalidad")
    df_all_nats = load_nationalities()
    options = df_all_nats["Nationality"].tolist()
    selected_nat = st.selectbox("Nacionalidad", options)
    limit_nat = st.slider("Límite por nacionalidad", 10, 100, 30, 10)

    df_nat_players = load_players_by_nationality(selected_nat, limit_nat)
    st.dataframe(df_nat_players, use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"No se pudo cargar el dashboard: {e}")
    st.info("Verifica que la BD 'fifa' y la tabla 'players' existan, y que PostgreSQL esté activo.")