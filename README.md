# FIFA ETL Pipeline

## 1. Objetivo

Este proyecto implementa un proceso ETL en Python para:

1. Extraer datos de jugadores desde un archivo CSV.
2. Transformar y normalizar los datos.
3. Cargar los datos en PostgreSQL.
4. Realizar adición de fotos de jugadores usando la API de Wikipedia.
5. Validar resultados mediante una interfaz en Streamlit.

El proyecto está orientado a ejecución local en Windows, con PostgreSQL en `localhost:5432`.

---

## 2. Estructura del proyecto

```text
fifa-etl-pipeline/
├─ data/
│  └─ raw/
│     └─ FullData.csv
├─ etl/
│  ├─ extract.py
│  ├─ transform.py
│  ├─ load.py
│  ├─ photos.py
│  └─ ui_app.py
├─ main.py
└─ requirements.txt
```

---

## 3. Flujo general del pipeline

`main.py` orquesta el flujo:

1. `extract_data()` en `etl/extract.py`
2. `transform_data(df)` en `etl/transform.py`
3. `load_data(df)` en `etl/load.py`
4. opcional: `enrich_player_photos(...)` en `etl/photos.py` (adición de fotos)

Flujo lógico:

```text
CSV -> DataFrame -> limpieza -> PostgreSQL (players) -> adición de fotos por API -> PostgreSQL (player_photos)
```

---

## 4. Explicación técnica por módulo

### 4.1 `main.py`

**Responsabilidad:** punto de entrada del ETL.

**Componentes:**
- `run_pipeline(with_photos=False, photo_limit=200)`:
  - ejecuta extracción, transformación y carga.
  - si `with_photos=True`, ejecuta adición de fotos.
- Argumentos CLI:
  - `--with-photos`: activa adición de fotos.
  - `--photo-limit`: cantidad de jugadores a consultar en API.

**Ejemplo:**
```bash
python main.py
python main.py --with-photos --photo-limit 100
```

---

### 4.2 `etl/extract.py`

**Responsabilidad:** lectura del CSV origen.

**Comportamiento:**
- Ruta esperada: `data/raw/FullData.csv`
- Separador: `;`
- Retorna `pandas.DataFrame`.

---

### 4.3 `etl/transform.py`

**Responsabilidad:** limpieza y normalización básica.

**Transformaciones:**
- Copia del DataFrame para evitar efectos laterales.
- Eliminación de duplicados.
- Limpieza de columnas tipo texto:
  - `strip()`
  - reemplazo de vacíos por `None`.

---

### 4.4 `etl/load.py`

**Responsabilidad:** conexión a PostgreSQL y carga de `players`.

**Configuración:** `DB_CONFIG` define usuario, contraseña, host, puerto y base de datos.

**Funciones principales:**
- `build_engine(database, autocommit=False)`: crea engine SQLAlchemy con `pg8000`.
- `ensure_database_exists()`: verifica/crea BD objetivo usando `AUTOCOMMIT`.
- `_safe_chunksize(...)`: evita exceder límite de parámetros del driver.
- `load_data(df)`:
  - valida estructura del DataFrame,
  - garantiza existencia de BD,
  - carga con `to_sql`,
  - usa fallback en caso de límite de parámetros.

---

### 4.5 `etl/photos.py`

**Responsabilidad:** adición de fotos de jugadores en `player_photos`.

**Tabla `player_photos`:**
- `id` (PK)
- `name`, `nationality`, `birth_date`
- `image_url`, `source`, `page_url`
- `status` (`found`, `not_found`, `error`)
- `error`, `fetched_at`
- restricción única: `(name, nationality, birth_date)`.

**Funciones principales:**
- `_create_table(engine)`: crea tabla si no existe.
- `_players(engine, limit)`: obtiene jugadores a procesar.
- `_search_candidates(...)`: consulta candidatos en Wikipedia API.
- `_pick_image(...)`: selecciona mejor candidato por relevancia.
- `_best_photo(...)`: aplica búsqueda principal y alternativa.
- `_upsert(...)`: inserta/actualiza resultados por jugador.
- `enrich_player_photos(...)`: orquesta la adición de fotos por lote.

---

### 4.6 `etl/ui_app.py`

**Responsabilidad:** validación visual de resultados.

**Funciones comunes en UI:**
- métricas de `players`,
- top jugadores por rating,
- listado con fotos mediante join `players` + `player_photos`.

---

## 5. Modelo de datos

### 5.1 `players`
Se genera desde CSV con `pandas.to_sql(..., if_exists="replace")`.

### 5.2 `player_photos`
Se genera con el proceso de adición de fotos por API.

Relación lógica:
- `players`: fuente principal
- `player_photos`: datos de foto por jugador

---

## 6. Requisitos

- Python 3.11+
- PostgreSQL local activo
- Dependencias:
  - `pandas`
  - `sqlalchemy`
  - `pg8000`
  - `requests`
  - `streamlit`

Instalación:

```bash
py -m pip install -r requirements.txt
```

---

## 7. Ejecución

### 7.1 ETL base
```bash
python main.py
```

### 7.2 ETL + adición de fotos
```bash
python main.py --with-photos --photo-limit 100
```

### 7.3 Dashboard
```bash
py -m streamlit run .\etl\ui_app.py
```

---

## 8. Consultas de validación SQL

Conteo de jugadores:
```sql
SELECT COUNT(*) FROM players;
```

Top jugadores por rating:
```sql
SELECT "Name", "Club", "Nationality", "Rating"
FROM players
ORDER BY "Rating" DESC, "Name" ASC
LIMIT 10;
```

Estado de adición de fotos:
```sql
SELECT status, COUNT(*)
FROM player_photos
GROUP BY status
ORDER BY status;
```

Registros con URL de imagen:
```sql
SELECT name, nationality, image_url
FROM player_photos
WHERE image_url IS NOT NULL
LIMIT 20;
```

---

## 9. Problemas comunes

1. `psql` o `streamlit` no reconocido:
   - ejecutar por módulo (`py -m streamlit ...`) o agregar binarios al PATH.

2. `CREATE DATABASE` dentro de transacción:
   - ejecutar creación de BD con `AUTOCOMMIT`.

3. Límite de parámetros en `pg8000`:
   - reducir `chunksize` y usar fallback sin `method="multi"`.

4. Sin fotos en UI:
   - validar estados en `player_photos` y volver a ejecutar con límite bajo:
   - `python main.py --with-photos --photo-limit 20`.

---

## 10. Mejoras recomendadas

- Configuración por variables de entorno (`.env`).
- Pruebas unitarias por módulo ETL.
- Migraciones de esquema (Alembic).
- Proceso asíncrono para adición de fotos.
- Estrategias de matching de nombres más robustas.
