import time
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import text

from etl.load import build_engine, DB_CONFIG

TABLE_PHOTOS = "player_photos"
USER_AGENT = "fifa-etl-pipeline/1.0"


def _create_table(engine):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_PHOTOS} (
        id BIGSERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        nationality TEXT,
        birth_date TEXT,
        image_url TEXT,
        source TEXT,
        page_url TEXT,
        status TEXT NOT NULL,
        error TEXT,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (name, nationality, birth_date)
    );
    """
    with engine.begin() as conn:
        conn.exec_driver_sql(sql)


def _players(engine, limit=200):
    sql = """
    WITH ranked AS (
        SELECT
            "Name" AS name,
            "Nationality" AS nationality,
            "Birth_Date"::text AS birth_date,
            "Rating" AS rating,
            ROW_NUMBER() OVER (
                PARTITION BY "Name", "Nationality", "Birth_Date"
                ORDER BY "Rating" DESC NULLS LAST, "Name" ASC
            ) AS rn
        FROM players
        WHERE "Name" IS NOT NULL
    )
    SELECT
        name,
        nationality,
        birth_date,
        rating
    FROM ranked
    WHERE rn = 1
    ORDER BY rating DESC NULLS LAST, name ASC
    LIMIT :limit
    """
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params={"limit": limit})


def _search_candidates(name: str, session: requests.Session):
    url = "https://en.wikipedia.org/w/api.php"
    params = {
        "action": "query",
        "generator": "search",
        "gsrsearch": f'intitle:"{name}" footballer',
        "gsrlimit": 5,
        "prop": "pageimages|pageterms|info",
        "piprop": "original|thumbnail",
        "pithumbsize": 400,
        "inprop": "url",
        "wbptterms": "description",
        "format": "json",
    }
    r = session.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    pages = data.get("query", {}).get("pages", {})
    return list(pages.values())


def _pick_image(candidates: list[dict], name: str, nationality: Optional[str] = None):
    best = None
    best_score = -1

    for page in candidates:
        title = (page.get("title") or "").lower()
        desc_list = page.get("terms", {}).get("description", [])
        desc = " ".join(desc_list).lower()
        thumb = (
            page.get("original", {}).get("source")
            or page.get("thumbnail", {}).get("source")
        )
        fullurl = page.get("fullurl")

        score = 0
        if name.lower() in title:
            score += 4
        if "footballer" in desc or "soccer player" in desc:
            score += 5
        if nationality and nationality.lower() in desc:
            score += 2
        if thumb:
            score += 3

        if score > best_score:
            best_score = score
            best = {
                "image_url": thumb,
                "page_url": fullurl,
                "status": "found" if thumb else "not_found",
                "error": None,
            }

    if best is None:
        return {
            "image_url": None,
            "page_url": None,
            "status": "not_found",
            "error": None,
        }

    return best


def _best_photo(name: str, nationality: Optional[str], session: requests.Session):
    candidates = _search_candidates(name, session)

    if not candidates and nationality:
        candidates = _search_candidates(f"{name} {nationality}", session)

    return _pick_image(candidates, name=name, nationality=nationality)


def _upsert(engine, row: dict):
    sql = text(f"""
        INSERT INTO {TABLE_PHOTOS}
        (name, nationality, birth_date, image_url, source, page_url, status, error, fetched_at)
        VALUES
        (:name, :nationality, :birth_date, :image_url, :source, :page_url, :status, :error, NOW())
        ON CONFLICT (name, nationality, birth_date)
        DO UPDATE SET
            image_url = EXCLUDED.image_url,
            source = EXCLUDED.source,
            page_url = EXCLUDED.page_url,
            status = EXCLUDED.status,
            error = EXCLUDED.error,
            fetched_at = NOW();
    """)
    with engine.begin() as conn:
        conn.execute(sql, row)


def enrich_player_photos(limit=200, sleep_seconds=0.2):
    print("PHOTO ENRICH iniciado", flush=True)

    engine = build_engine(DB_CONFIG["database"])
    _create_table(engine)
    df = _players(engine, limit=limit)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    found = 0
    total = len(df)

    try:
        for i, p in df.iterrows():
            name = p["name"]
            nationality = p.get("nationality")
            birth_date = p.get("birth_date")

            print(f"[{i + 1}/{total}] {name}", flush=True)

            try:
                result = _best_photo(name, nationality, session)
                payload = {
                    "name": name,
                    "nationality": nationality,
                    "birth_date": birth_date,
                    "image_url": result["image_url"],
                    "source": "wikipedia",
                    "page_url": result["page_url"],
                    "status": result["status"],
                    "error": result["error"],
                }
                _upsert(engine, payload)

                if result["status"] == "found":
                    found += 1
                    print("  -> foto encontrada", flush=True)
                else:
                    print("  -> sin foto", flush=True)

            except Exception as e:
                _upsert(
                    engine,
                    {
                        "name": name,
                        "nationality": nationality,
                        "birth_date": birth_date,
                        "image_url": None,
                        "source": "wikipedia",
                        "page_url": None,
                        "status": "error",
                        "error": str(e)[:500],
                    },
                )
                print(f"  -> error: {e}", flush=True)

            time.sleep(sleep_seconds)

    finally:
        session.close()
        engine.dispose()

    print(f"PHOTO ENRICH finalizado | total={total} | found={found}", flush=True)