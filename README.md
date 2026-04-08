# ETL-FIFA — AWS Data Pipeline

Python ETL pipeline that extracts FIFA player data from Amazon S3, transforms it, loads it into PostgreSQL, and writes the processed output back to S3. Deployable on EC2 or locally via Docker Compose.

---

## Architecture

```
S3 (raw/FullData.csv)
        │
        ▼
  EC2 / Docker
  ┌─────────────────────────────┐
  │  Extract  →  Transform  →  Load  │
  │  (boto3)     (pandas)    (SQLAlchemy + PostgreSQL)  │
  └─────────────────────────────┘
        │
        ▼
S3 (processed/players_output.csv)
```

### Components

| Module | File | Responsibility |
|--------|------|----------------|
| Extract | `etl/extract.py` | Downloads source CSV from S3 (or reads locally) |
| Transform | `etl/transform.py` | Cleans and normalizes 17,588 player records |
| Load | `etl/load.py` | Writes to PostgreSQL via SQLAlchemy |
| S3 Utils | `etl/s3_utils.py` | boto3 helpers for download and upload |
| Photos | `etl/photos.py` | Optional: enriches players with photo URLs |
| Entrypoint | `main.py` | Pipeline orchestrator with CLI flags |

---

## Infrastructure

- **Compute:** EC2 `t3.micro` — Ubuntu 22.04, `us-east-2`
- **Storage:** S3 bucket `etl-fifa-felipe`
  - `raw/FullData.csv` — source data (3.7 MB, 17,588 rows)
  - `processed/players_output.csv` — pipeline output
- **Database:** PostgreSQL 14 (local on EC2 or via Docker Compose)
- **Auth:** IAM Role `EC2-S3-ETL-FIFA` with `AmazonS3FullAccess` — no hardcoded credentials, boto3 picks up the instance profile automatically via IMDSv2

---

## S3 Integration

The pipeline detects the `S3_BUCKET` environment variable at runtime:

```python
# etl/extract.py
def extract_data() -> pd.DataFrame:
    if os.getenv("S3_BUCKET"):
        from etl.s3_utils import download_csv_from_s3
        local_path = os.path.join(tempfile.gettempdir(), "FullData.csv")
        path = download_csv_from_s3(local_path)
    else:
        path = os.getenv("DATA_CSV_PATH", str(DEFAULT_CSV_PATH))
    return pd.read_csv(path, sep=";")
```

If `S3_BUCKET` is not set, the pipeline falls back to a local file path — making it runnable in both environments without code changes.

After `load_data()`, the processed DataFrame is uploaded back to S3:

```python
# main.py
if os.getenv("S3_BUCKET"):
    from etl.s3_utils import upload_processed_to_s3
    output_path = os.path.join(tempfile.gettempdir(), "players_output.csv")
    df.to_csv(output_path, index=False)
    upload_processed_to_s3(output_path)
```

---

## Running on EC2

### Prerequisites

- EC2 instance with IAM Role `EC2-S3-ETL-FIFA` attached
- PostgreSQL running locally (`sudo systemctl start postgresql`)
- Python 3.10+

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run pipeline

```bash
export S3_BUCKET=etl-fifa-felipe
export AWS_REGION=us-east-2
export DB_HOST=localhost
export DB_PASSWORD=admin

python3 main.py
```

### Expected output

```
PIPELINE START
Downloaded s3://etl-fifa-felipe/raw/FullData.csv -> /tmp/FullData.csv
TRANSFORM iniciado
Filas después de limpiar: (17588, 53)
LOAD iniciado
Base de datos creada: fifa
Datos cargados
Uploaded /tmp/players_output.csv -> s3://etl-fifa-felipe/processed/players_output.csv
PIPELINE END
```

### Optional: enrich with player photos

```bash
python3 main.py --with-photos --photo-limit 200
```

---

## Running locally with Docker Compose

```bash
docker compose up
```

The `compose.yaml` spins up the app and a PostgreSQL container. The pipeline reads the CSV from `data/raw/FullData.csv` (no S3 required locally).

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | — | S3 bucket name. If set, triggers S3 mode |
| `S3_SOURCE_KEY` | `raw/FullData.csv` | S3 key for source CSV |
| `S3_OUTPUT_KEY` | `processed/players_output.csv` | S3 key for processed output |
| `AWS_REGION` | `us-east-1` | AWS region |
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `fifa` | Database name |
| `DB_USER` | `postgres` | Database user |
| `DB_PASSWORD` | `admin` | Database password |
| `DB_AUTO_CREATE` | `true` | Auto-create DB if it doesn't exist |
| `DATA_CSV_PATH` | `data/raw/FullData.csv` | Local CSV path (fallback when no S3) |
| `WITH_PHOTOS` | `false` | Enable photo enrichment |
| `PHOTO_LIMIT` | `200` | Max players to enrich with photos |

---

## Data

Source: [FIFA Complete Player Dataset — Kaggle](https://www.kaggle.com/)

- **Rows:** 17,588 players
- **Columns:** 53 attributes (ratings, positions, physical stats, club info)
- **Format:** CSV with `;` delimiter

---

## Security Notes

- No AWS credentials are stored in code or environment files
- The EC2 instance authenticates to AWS via an IAM Instance Profile (IMDSv2)
- The S3 bucket has public access blocked; only the EC2 role can read/write
- SSH access to the EC2 is restricted to a specific IP via Security Group inbound rules

---

## Branch Strategy

| Branch | Purpose |
|--------|---------|
| `main` | Stable local + Docker version |
| `aws-version` | EC2 + S3 deployment (this branch) |
