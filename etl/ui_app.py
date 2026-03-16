import pandas as pd
import streamlit as st
from sqlalchemy import text

from etl.load import build_engine, DB_CONFIG

st.set_page_config(page_title="FIFA ETL - Dashboard", layout="wide")
st.title("⚽ FIFA ETL - Dashboard de validación")

TABLE_NAME = "players"
TABLE_PHOTOS = "player_photos"


@st.cache_resource
def get_engine():
    return build_engine(DB_CONFIG["database"])


def run_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


@st.cache_data(ttl=60)
def load_summary():
    sql = f"""
    SELECT
      COUNT(*)::int AS total_rows,
      COUNT("Rating")::int AS rating_not_null,
      COALESCE(MAX("Rating"), 0)::int AS max_rating,
      COALESCE(AVG("Rating"), 0)::numeric(10,2) AS avg_rating
    FROM "{TABLE_NAME}";
    """
    return run_query(sql)


@st.cache_data(ttl=60)
def load_top_players(limit: int):
    sql = f"""
    SELECT
      "Name",
      "Club",
      "Nationality",
      "Rating"::int AS rating
    FROM "{TABLE_NAME}"
    WHERE "Rating" IS NOT NULL
    ORDER BY "Rating" DESC, "Name" ASC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})


@st.cache_data(ttl=60)
def load_players_with_photos(limit: int):
    sql = f"""
    SELECT
      p."Name",
      p."Club",
      p."Nationality",
      p."Rating"::int AS rating,
      ph.image_url
    FROM "{TABLE_NAME}" p
    LEFT JOIN "{TABLE_PHOTOS}" ph
      ON ph.name = p."Name"
    WHERE p."Rating" IS NOT NULL
    ORDER BY p."Rating" DESC, p."Name" ASC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})


try:
    summary = load_summary().iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total filas", int(summary["total_rows"]))
    c2.metric("Ratings válidos", int(summary["rating_not_null"]))
    c3.metric("Rating máximo", int(summary["max_rating"]))
    c4.metric("Rating promedio", float(summary["avg_rating"]))

    st.divider()

    top_n = st.slider("Top jugadores", 5, 50, 10, 5)
    df_top = load_top_players(top_n)
    st.subheader("🏆 Top jugadores por rating")
    st.dataframe(df_top, use_container_width=True, hide_index=True)

    st.divider()

    st.subheader("🖼️ Top jugadores con foto (si existe)")
    photo_n = st.slider("Cantidad con foto", 5, 30, 10, 5)
    df_photo = load_players_with_photos(photo_n)

    for _, row in df_photo.iterrows():
        with st.container(border=True):
            cols = st.columns([1, 3])
            with cols[0]:
                if row.get("image_url"):
                    st.image(row["image_url"], width=110)
                else:
                    st.caption("Sin foto")
            with cols[1]:
                st.write(f"**{row['Name']}**")
                st.write(f"Club: {row['Club']}")
                st.write(f"Nacionalidad: {row['Nationality']}")
                st.write(f"Rating: {row['rating']}")

except Exception as e:
    st.error(f"No se pudo cargar el dashboard: {e}")
    st.info("Verifica PostgreSQL, la BD 'fifa' y la tabla 'players'.")