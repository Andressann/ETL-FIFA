# FIFA ETL Pipeline

## 1. Objetivo

Este proyecto implementa un proceso ETL en Python para:

1. Extraer datos de jugadores desde un archivo CSV.
2. Transformar y normalizar los datos.
3. Cargar los datos en PostgreSQL.
4. Enriquecer jugadores con fotos usando la API de Wikipedia.
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
4. opcional: `enrich_player_photos(...)` en `etl/photos.py`

Flujo lógico:

```text
CSV -> DataFrame -> limpieza -> PostgreSQL (players) -> enriquecimiento API -> PostgreSQL (player_photos)
```

---

## 4. Explicación técnica por módulo

## 4.1 `main.py`

### Responsabilidad
Punto de entrada del proceso ETL.

### Componentes
- `run_pipeline(with_photos=False, photo_limit=200)`:
  - ejecuta extracción, transformación y carga.
  - si `with_photos=True`, ejecuta enriquecimiento de fotos.
- Argumentos de línea de comandos:
  - `--with-photos`: activa enriquecimiento.
  - `--photo-limit`: cantidad de jugadores a consultar en API.

### Ejemplo
```bash
python main.py
python main.py --with-photos --photo-limit 100
```

---

## 4.2 `etl/extract.py`

### Responsabilidad
Leer archivo fuente CSV y cargarlo en un DataFrame.

### Comportamiento
- Ruta esperada: `data/raw/FullData.csv`
- Separador: `;`
- Retorna `pandas.DataFrame`.

### Riesgos cubiertos
- Si el separador es incorrecto, el DataFrame queda con 1 sola columna; esto se valida en `load.py`.

---

## 4.3 `etl/transform.py`

### Responsabilidad
Aplicar limpieza básica y normalización.

### Transformaciones
- Clona el DataFrame (`copy`) para evitar efectos laterales.
- Elimina duplicados.
- Normaliza columnas de texto:
  - `strip()`
  - reemplazo de vacíos por `None`.

### Resultado
DataFrame limpio y consistente para carga en base de datos.

---

## 4.4 `etl/load.py`

### Responsabilidad
Gestionar conexión a PostgreSQL y persistencia en tabla `players`.

### Configuración
`DB_CONFIG` define:
- usuario
- contraseña
- host
- puerto
- base de datos objetivo (`fifa`)

### Funciones principales

#### `build_engine(database, autocommit=False)`
Crea `SQLAlchemy Engine` con driver `postgresql+pg8000`.

#### `ensure_database_exists()`
- Se conecta a `postgres` en modo `AUTOCOMMIT`.
- Verifica si existe la BD objetivo.
- Si no existe, ejecuta `CREATE DATABASE`.

#### `_safe_chunksize(n_cols, max_params=60000, max_rows=1000)`
Calcula un `chunksize` seguro para evitar superar el límite de parámetros del protocolo (`pg8000`).

#### `load_data(df)`
- Valida estructura del DataFrame.
- Asegura existencia de BD.
- Inserta en tabla `players` con `to_sql`.
- Usa `method="multi"` y `chunksize` seguro.
- Implementa fallback de inserción por lotes sin `multi` si aparece error de límite de parámetros.

### Consideración crítica
Con muchas columnas y lotes grandes, `pg8000` puede fallar por exceso de parámetros. El cálculo dinámico de `chunksize` evita este problema.

---

## 4.5 `etl/photos.py`

### Responsabilidad
Enriquecer jugadores con URL de imagen desde Wikipedia y guardar resultado en `player_photos`.

### Diseño de tabla `player_photos`
- `id` (PK)
- `name`, `nationality`, `birth_date`
- `image_url`, `source`, `page_url`
- `status` (`found`, `not_found`, `error`)
- `error`
- `fetched_at`
- `UNIQUE (name, nationality, birth_date)` para idempotencia lógica.

### Funciones principales

#### `_create_table(engine)`
Crea la tabla si no existe.

#### `_players(engine, limit)`
Selecciona jugadores desde `players`, deduplicando con `ROW_NUMBER()`:
- partición por nombre/nacionalidad/fecha.
- se conserva el registro con mayor rating.

#### `_search_candidates(name, session)`
Consulta Wikipedia API (`action=query`, `generator=search`) y devuelve páginas candidatas con metadatos e imágenes.

#### `_pick_image(candidates, name, nationality)`
Asigna score por relevancia:
- coincidencia de nombre en título
- descripción compatible (`footballer`, `soccer player`)
- coincidencia de nacionalidad
- existencia de imagen

Selecciona el mejor candidato.

#### `_best_photo(name, nationality, session)`
Ejecuta búsqueda principal y, si no hay candidatos, intenta búsqueda alternativa con nacionalidad.

#### `_upsert(engine, row)`
`INSERT ... ON CONFLICT DO UPDATE` para actualizar resultados sin duplicar.

#### `enrich_player_photos(limit=200, sleep_seconds=0.2)`
Orquesta el enriquecimiento:
- recorre jugadores
- consulta API
- persiste resultado por jugador
- registra progreso en consola
- controla errores por fila para no abortar todo el lote.

### Razones del enfoque
- Idempotencia por clave única.
- Tolerancia a fallos de red/API.
- Reejecución segura del proceso de enriquecimiento.

---

## 4.6 `etl/ui_app.py`

### Responsabilidad
Visualizar métricas y validar resultados del ETL en Streamlit.

### Funcionalidad típica
- métricas de tabla `players` (conteo, rating promedio, máximo).
- listado top jugadores por rating.
- listado de jugadores con foto (join `players` + `player_photos`).
- render de imagen cuando `image_url` existe.

### Nota
La UI no reemplaza pruebas automáticas; se usa como validación operativa rápida.

---

## 5. Modelo de datos

## 5.1 Tabla `players`
Se crea a partir del CSV con `pandas.to_sql(..., if_exists="replace")`.

## 5.2 Tabla `player_photos`
Resultado del enriquecimiento por API.

Relación lógica:
- `players` (fuente principal)
- `player_photos` (enriquecimiento por entidad de jugador)

---

## 6. Requisitos

- Python 3.11+ (probado también en 3.14)
- PostgreSQL local activo
- Dependencias Python:
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

## 7. Configuración por variables de entorno

El proyecto ya no depende de valores fijos para base de datos o ruta del CSV.

Variables soportadas:

- `DB_HOST`: host de PostgreSQL.
- `DB_PORT`: puerto de PostgreSQL.
- `DB_NAME`: nombre de la base de datos objetivo.
- `DB_USER`: usuario.
- `DB_PASSWORD`: contraseña.
- `DB_ADMIN_DATABASE`: base administrativa usada para `CREATE DATABASE` cuando aplica.
- `DB_AUTO_CREATE`: `true` o `false`. En AWS conviene usar `false` si la base ya existe en RDS.
- `DATA_CSV_PATH`: ruta del archivo CSV. Por defecto apunta a `data/raw/FullData.csv` dentro del proyecto.

Ejemplo local:

```bash
set DB_HOST=localhost
set DB_PORT=5432
set DB_NAME=fifa
set DB_USER=postgres
set DB_PASSWORD=admin
set DB_AUTO_CREATE=true
python main.py
```

---

## 8. Despliegue simple en AWS con Docker

Esta opción está pensada para un despliegue rápido, sin reestructurar el proyecto.

Arquitectura recomendada:

1. **Amazon RDS PostgreSQL** para la base de datos.
2. **AWS App Runner** o **ECS Fargate** para correr el contenedor.
3. El CSV puede viajar dentro de la imagen si no cambia frecuentemente.

### 8.1 Construcción local de la imagen

```bash
docker build -t fifa-etl-pipeline .
```

### 8.2 Ejecutar el ETL en contenedor

```bash
docker run --rm \
  -e APP_MODE=etl \
  -e DB_HOST=<rds-endpoint> \
  -e DB_PORT=5432 \
  -e DB_NAME=fifa \
  -e DB_USER=<usuario> \
  -e DB_PASSWORD=<password> \
  -e DB_AUTO_CREATE=false \
  fifa-etl-pipeline
```

### 8.3 Ejecutar Streamlit con la misma imagen

```bash
docker run --rm -p 8080:8080 \
  -e APP_MODE=streamlit \
  -e DB_HOST=<rds-endpoint> \
  -e DB_PORT=5432 \
  -e DB_NAME=fifa \
  -e DB_USER=<usuario> \
  -e DB_PASSWORD=<password> \
  -e DB_AUTO_CREATE=false \
  -e PORT=8080 \
  fifa-etl-pipeline
```

### 8.4 Recomendaciones operativas en AWS

- No hardcodear credenciales; usar variables de entorno o AWS Secrets Manager.
- En RDS, crear la base `fifa` previamente y desplegar con `DB_AUTO_CREATE=false`.
- Si el CSV cambia seguido, el siguiente paso lógico es moverlo a S3 y leerlo desde ahí.
- Para App Runner, usar `APP_MODE=streamlit` y puerto `8080` si el servicio será la UI.

### 8.5 Modos disponibles en la imagen

La imagen ahora soporta dos modos de ejecución mediante `APP_MODE`:

- `APP_MODE=etl`: ejecuta `main.py`.
- `APP_MODE=streamlit`: levanta `etl/ui_app.py` en `0.0.0.0:$PORT`.

Variables adicionales útiles:

- `WITH_PHOTOS=true`: activa enriquecimiento de fotos en modo ETL.
- `PHOTO_LIMIT=100`: controla el límite de jugadores al enriquecer.

### 8.6 Flujo mínimo de despliegue en AWS

1. Crear un repositorio en Amazon ECR.
2. Construir y subir la imagen.
3. Crear un servicio en App Runner para el dashboard con `APP_MODE=streamlit`.
4. Ejecutar el ETL como tarea puntual en ECS/Fargate o manualmente con la misma imagen usando `APP_MODE=etl`.

### 8.7 Despliegue recomendado en EC2

Si tu idea es usar **EC2**, esta es una ruta razonable y simple para este proyecto.

Arquitectura sugerida:

1. Una instancia EC2 con Docker y Docker Compose.
2. La aplicación corriendo en contenedor con `APP_MODE=streamlit`.
3. PostgreSQL externo, idealmente en **Amazon RDS**.
4. El puerto público expuesto en EC2 sería `80`.

Archivo incluido para este caso:

- `compose.yaml`: publica `80 -> 8080` y levanta la app con reinicio automático.

Pasos en EC2:

```bash
sudo apt update
sudo apt install -y docker.io docker-compose-plugin git
sudo usermod -aG docker $USER
newgrp docker

git clone <tu-repo>
cd fifa-etl-pipeline
cp .env.example .env
```

Editar `.env` con valores reales:

```bash
APP_MODE=streamlit
PORT=8080
DB_HOST=<endpoint-rds-o-host-remoto>
DB_PORT=5432
DB_NAME=fifa
DB_USER=<usuario>
DB_PASSWORD=<password>
DB_AUTO_CREATE=false
```

Levantar la aplicación:

```bash
docker compose up -d --build
```

Verificar estado:

```bash
docker compose ps
docker compose logs -f
```

Abrir en navegador:

```text
http://<ip-publica-ec2>
```

Reglas mínimas de Security Group:

- `22` para SSH.
- `80` para acceso web.
- No exponer `5432` públicamente si usas RDS; mejor tráfico privado o reglas restringidas.

Si después quieres correr el ETL dentro de la misma EC2, puedes lanzar una ejecución puntual con:

```bash
docker compose run --rm -e APP_MODE=etl fifa-etl-pipeline
```

Y si quieres enriquecer fotos:

```bash
docker compose run --rm -e APP_MODE=etl -e WITH_PHOTOS=true -e PHOTO_LIMIT=100 fifa-etl-pipeline
```

---

## 9. Ejecución

## 9.1 ETL base
```bash
python main.py
```

## 9.2 ETL + fotos
```bash
python main.py --with-photos --photo-limit 100
```

## 9.3 Dashboard
```bash
py -m streamlit run .\etl\ui_app.py
```

---

## 10. Consultas de validación SQL

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

Estado de enriquecimiento de fotos:
```sql
SELECT status, COUNT(*)
FROM player_photos
GROUP BY status
ORDER BY status;
```

Muestras con URL disponible:
```sql
SELECT name, nationality, image_url
FROM player_photos
WHERE image_url IS NOT NULL
LIMIT 20;
```

---

## 11. Problemas comunes y resolución

1. **`psql` o `streamlit` no reconocido**
   - Ejecutar vía módulo:
   - `py -m streamlit run ...`
   - usar ruta completa de `psql.exe` si no está en PATH.

2. **`CREATE DATABASE ... dentro de un bloque de transacción`**
   - resolver usando conexión en `AUTOCOMMIT` (ya implementado).

3. **Error de parámetros en `pg8000` (`'H' format requires ...`)**
   - resolver con `chunksize` seguro y fallback sin `method="multi"` (ya implementado).

4. **`SELECT DISTINCT` con `ORDER BY`**
   - en PostgreSQL, las columnas de `ORDER BY` deben estar en el `SELECT` cuando hay `DISTINCT`.
   - se resolvió con deduplicación por `ROW_NUMBER()`.

5. **Sin fotos en UI**
   - revisar `status` en `player_photos`.
   - ejecutar enriquecimiento con límite bajo para depurar:
     - `python main.py --with-photos --photo-limit 20`

---

## 12. Buenas prácticas aplicadas

- Separación por capas ETL (`extract`, `transform`, `load`).
- Reintentos operativos por lote en enriquecimiento.
- Persistencia idempotente por `upsert`.
- Logs de progreso para trazabilidad.
- Configuración centralizada de conexión.

---

## 13. Mejoras sugeridas

- Variables de entorno para credenciales (`.env`).
- Pruebas unitarias por módulo.
- Versionado de esquema con migraciones (Alembic).
- Cola asíncrona para enriquecimiento de fotos.
- Reglas de matching más robustas (normalización de nombres y aliases).