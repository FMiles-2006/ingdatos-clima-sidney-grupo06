"""
Configuración del entorno — Proyecto integrador grupo06 (Clima de Sídney).
Ingeniería de Datos, LCD UCA Rosario 2026.

Basado en el entorno.py de la cátedra: el MISMO código funciona en el servidor de la
cátedra y en el Docker local. Todo se lee de variables de entorno; si no están, se usan
los valores por defecto del stack local.

Orden de resolución de cada valor:
    1) variable de entorno ya presente (la que inyecta JupyterHub / Airflow)
    2) archivo .env junto a este entorno.py (NO se versiona: está en .gitignore)
    3) valor por defecto del stack local (docker compose de la cátedra)

Uso:
    from entorno import BRONZE, STORAGE_OPTIONS, DUCKDB_PATH, PG, pg_url, PG_SCHEMA, info
"""
import os

# ---- Raíz del proyecto: la carpeta donde vive este archivo ----
BASE = os.environ.get("CURSO_DIR") or os.path.dirname(os.path.abspath(__file__))


def _cargar_dotenv(base):
    """Carga BASE/.env en os.environ SIN pisar variables ya definidas."""
    ruta = os.path.join(base, ".env")
    if not os.path.exists(ruta):
        return
    for linea in open(ruta, encoding="utf-8"):
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        os.environ.setdefault(clave.strip(), valor.strip())


_cargar_dotenv(BASE)

# ---- Identidad del grupo ----
GROUP = os.environ.get("GRUPO") or os.environ.get("MINIO_BUCKET") or "local"

# ---- Rutas del material (relativas a BASE) ----
DBT_DIR = os.environ.get("DBT_DIR", os.path.join(BASE, "dbt"))
DAGS_DIR = os.environ.get("DAGS_DIR", os.path.join(BASE, "dags"))
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", os.path.join(DBT_DIR, "warehouse.duckdb"))

# ---- Object storage (MinIO / S3) ----
BUCKET = os.environ.get("MINIO_BUCKET", "lake")          # servidor: grupo06 · local: lake
BRONZE = os.environ.get("BRONZE_PATH", f"s3://{BUCKET}/bronze/clima_sidney")
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", os.environ.get("MINIO_ACCESS_KEY", "minioadmin")),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", os.environ.get("MINIO_SECRET_KEY", "minioadmin")),
    "AWS_ENDPOINT_URL": os.environ.get("AWS_ENDPOINT_URL", os.environ.get("MINIO_ENDPOINT", "http://minio:9000")),
    "AWS_REGION": os.environ.get("AWS_REGION", "us-east-1"),
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# ---- Warehouse de serving (PostgreSQL, capa gold) ----
PG = {
    "host": os.environ.get("PGHOST", "postgres"),
    "port": os.environ.get("PGPORT", "5432"),
    "dbname": os.environ.get("PGDATABASE", "gold"),       # servidor: warehouse · local: gold
    "user": os.environ.get("PGUSER", "airflow"),          # servidor: grupo06 · local: airflow
    "password": os.environ.get("PGPASSWORD", "airflow"),
}
# En el servidor cada grupo tiene su schema (grupo06); en local usamos 'public'.
PG_SCHEMA = os.environ.get("PGSCHEMA") or (GROUP if GROUP != "local" else "public")


def pg_url():
    return (f"postgresql+psycopg2://{PG['user']}:{PG['password']}"
            f"@{PG['host']}:{PG['port']}/{PG['dbname']}")


def info():
    entorno = "servidor de la cátedra" if os.environ.get("MINIO_BUCKET") else "Docker local"
    print(f"Entorno detectado : {entorno}  (grupo: {GROUP})")
    print(f"  Bronze (Delta)  : {BRONZE}")
    print(f"  DuckDB (dbt)    : {DUCKDB_PATH}")
    print(f"  PostgreSQL      : base '{PG['dbname']}', schema '{PG_SCHEMA}', usuario '{PG['user']}'")
    print(f"  Material en     : {BASE}")
