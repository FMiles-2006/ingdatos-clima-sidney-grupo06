"""
Genera los notebooks NB00-NB06 del proyecto a partir de este archivo (fuente única,
fácil de revisar en un diff). Uso:  python notebooks/_generar_notebooks.py
"""
import json
import os

AQUI = os.path.dirname(os.path.abspath(__file__))

PREP = '''import os, sys
from pathlib import Path
# Raíz del proyecto = carpeta padre de notebooks/ (en el servidor: ~/work/dags)
RAIZ = Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()
sys.path.insert(0, str(RAIZ))
os.chdir(RAIZ)
import entorno
entorno.info()'''


def md(t):
    return {"cell_type": "markdown", "metadata": {}, "source": t.strip("\n")}


def code(t):
    return {"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": t.strip("\n")}


def nb(nombre, celdas):
    doc = {"cells": celdas, "metadata": {
        "kernelspec": {"display_name": "Python 3 (ipykernel)", "language": "python", "name": "python3"},
        "language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}
    for i, c in enumerate(doc["cells"]):
        c["id"] = f"{nombre[:4].lower()}-{i:02d}"
    with open(os.path.join(AQUI, nombre), "w", encoding="utf-8") as f:
        json.dump(doc, f, ensure_ascii=False, indent=1)
    print("ok", nombre)


CABECERA = "**Ingeniería de Datos — LCD · UCA Rosario · 2026** · grupo06 · Clima de Sídney (Open-Meteo)"

# ---------------------------------------------------------------------------
nb("NB00_desplegar_en_servidor.ipynb", [
    md(f"""# NB00 · Desplegar y actualizar el proyecto en el servidor de la cátedra
{CABECERA}

El Airflow compartido **solo monta la carpeta `~/work/dags`** de nuestro workspace
(la ve como `/opt/airflow/dags/grupo06`). Por eso el repositorio completo se clona *dentro*
de `~/work/dags`, y el `.airflowignore` hace que Airflow solo parsee `dags/pipeline_grupo06.py`.

Este notebook:
1. clona o actualiza (`git pull`) el repositorio en `~/work/dags`,
2. genera el `.env` del grupo **a partir de las variables que JupyterHub ya inyecta** (no se escriben claves a mano y el `.env` nunca se versiona),
3. verifica MinIO y PostgreSQL."""),
    code('''import os, subprocess
REPO = "https://github.com/FMiles-2006/ingdatos-clima-sidney-grupo06.git"
DESTINO = os.path.expanduser("~/work/dags")

def sh(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(r.stdout[-3000:], r.stderr[-2000:])
    return r.returncode

if os.path.isdir(os.path.join(DESTINO, ".git")):
    sh(f"cd {DESTINO} && git pull --ff-only")
else:
    # Primera vez: la carpeta puede no estar vacía (Airflow deja __pycache__), por eso init + fetch.
    sh(f"cd {DESTINO} && git init -q -b main && git remote add origin {REPO} && git fetch -q origin main && git checkout -f -B main origin/main")
sh(f"cd {DESTINO} && git log --oneline -5")'''),
    md("## 2. `.env` del grupo (credenciales fuera del repositorio)"),
    code('''e = os.environ
valores = {
    "GRUPO": "grupo06", "MINIO_BUCKET": e["MINIO_BUCKET"],
    "AWS_ACCESS_KEY_ID": e["AWS_ACCESS_KEY_ID"], "AWS_SECRET_ACCESS_KEY": e["AWS_SECRET_ACCESS_KEY"],
    "AWS_ENDPOINT_URL": e.get("AWS_ENDPOINT_URL", "http://minio:9000"), "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
    "PGHOST": e["PGHOST"], "PGPORT": e["PGPORT"], "PGDATABASE": e["PGDATABASE"],
    "PGUSER": e["PGUSER"], "PGPASSWORD": e["PGPASSWORD"], "PGSCHEMA": "grupo06",
}
with open(os.path.join(DESTINO, ".env"), "w") as f:
    f.write("# Generado por NB00 desde las variables del Jupyter de grupo06. NO versionar.\\n")
    f.writelines(f"{k}={v}\\n" for k, v in valores.items())
print("Claves escritas en .env:", list(valores))   # solo los nombres, nunca los valores
sh(f"cd {DESTINO} && git check-ignore -v .env")      # confirma que git lo ignora'''),
    md("## 3. Verificar el stack"),
    code(PREP.replace('Path.cwd().parent if Path.cwd().name == "notebooks" else Path.cwd()', 'Path(DESTINO)') + '''
import boto3
from sqlalchemy import create_engine, text
so = entorno.STORAGE_OPTIONS
s3 = boto3.client("s3", endpoint_url=so["AWS_ENDPOINT_URL"], aws_access_key_id=so["AWS_ACCESS_KEY_ID"],
                  aws_secret_access_key=so["AWS_SECRET_ACCESS_KEY"], region_name="us-east-1")
r = s3.list_objects_v2(Bucket=entorno.BUCKET, Prefix="bronze/clima_sidney/_delta_log/")
print("MinIO OK · versiones del log de bronze:", r.get("KeyCount", 0))
with create_engine(entorno.pg_url()).connect() as c:
    print("PostgreSQL OK ·", c.execute(text("select current_user, current_database(), current_schema()")).fetchone())'''),
])

# ---------------------------------------------------------------------------
nb("NB01_semana02_recorrido_stack.ipynb", [
    md(f"""# NB01 · Semana 2 — Recorrido del stack de datos
{CABECERA}

Cada pieza del stack cubre una etapa del ciclo de vida. En el servidor de la cátedra:

| Componente | Etapa | Dónde |
|---|---|---|
| MinIO | Almacenamiento (data lake, S3) | bucket `grupo06` |
| DuckDB | Transformación (motor in-process de dbt) | `dbt/warehouse.duckdb` |
| Airflow | Orquestación | https://airflow.nexoacademia.org |
| PostgreSQL | Serving (capa gold) | base `warehouse`, schema `grupo06` |
| Metabase | BI | https://metabase.nexoacademia.org |"""),
    code(PREP),
    md("## 1. MinIO: el lake\nNos conectamos por protocolo S3 con las credenciales del grupo (variables de entorno, nunca escritas en el código). Solo tenemos permiso sobre **nuestro** bucket: listar otros buckets da 403, y eso es aislamiento entre grupos, no un error."),
    code('''import boto3
so = entorno.STORAGE_OPTIONS
s3 = boto3.client("s3", endpoint_url=so["AWS_ENDPOINT_URL"], aws_access_key_id=so["AWS_ACCESS_KEY_ID"],
                  aws_secret_access_key=so["AWS_SECRET_ACCESS_KEY"], region_name="us-east-1")
r = s3.list_objects_v2(Bucket=entorno.BUCKET, Prefix="bronze/clima_sidney/", MaxKeys=20)
for o in r.get("Contents", []):
    print(f"{o['Size']:>9}  {o['Key']}")'''),
    md("## 2. DuckDB: motor analítico in-process\nNo es un servidor: corre dentro de este proceso. dbt lo usa para transformar."),
    code('''import duckdb
print("DuckDB", duckdb.__version__)
duckdb.sql("select 42 as respuesta, current_date as hoy").show()'''),
    md("## 3. PostgreSQL: warehouse de serving\nAcá publicamos la capa gold para que Metabase la consulte."),
    code('''from sqlalchemy import create_engine, text
with create_engine(entorno.pg_url()).connect() as con:
    print(con.execute(text("select version()")).scalar())
    print(con.execute(text("select current_schema()")).scalar())'''),
])

# ---------------------------------------------------------------------------
nb("NB02_semana04_extraccion_bronze.ipynb", [
    md(f"""# NB02 · Semana 4 — Extracción de Open-Meteo hacia bronze (Entrega 1)
{CABECERA}

El pipeline de ingesta vive en `scripts/ingesta.py` (lo usan este notebook **y** Airflow: una sola implementación).
Acá lo recorremos paso a paso y hacemos la **demo de E1**: correr dos veces y mostrar que la segunda no agrega nada.

**Dos APIs de Open-Meteo**
| Rango | API | Por qué |
|---|---|---|
| 2022-01-01 → 2025-12-31 | Archive (reanálisis ERA5) | Histórico de máxima calidad |
| 2026-01-01 → ayer | Historical Forecast | Tiene el día anterior completo (ERA5 se publica con días de demora) |"""),
    code(PREP + '''
import logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)
from scripts import ingesta
from datetime import date, timedelta'''),
    md("""## 1. Watermark
La máxima `fecha` que ya está en bronze. Se **deriva de los datos** (no de un archivo aparte que se pueda desincronizar)
y se usa como **parámetro de la consulta**: `start_date = watermark + 1`. Si la tabla no existe devuelve `None` (primera corrida);
cualquier otro error (credenciales, red) se propaga, para no confundir "falla" con "tabla vacía" y recargar todo."""),
    code("print('watermark actual:', ingesta.leer_watermark())"),
    md("""## 2. Extracción con manejo de errores
`extraer(desde, hasta)` parte el rango por fuente y por año, y por cada pedido:
- **verifica el status antes de parsear**;
- **transitorias** (timeout, sin conexión, HTTP 429, 5xx) → hasta 3 intentos con espera 2 s / 4 s;
- **permanentes** (HTTP 400/401/403/404, cuerpo que no es JSON, falta `daily`, columnas de distinto largo, fechas fuera de rango, unidad distinta de °C) → error inmediato, sin reintentar;
- días con **todas** las mediciones nulas (aún no publicados) no se escriben: el watermark no avanza y se vuelven a pedir.

Probamos con una semana (no escribe nada):"""),
    code('''muestra = ingesta.extraer(date(2025, 12, 29), date(2026, 1, 3))
muestra[["fecha", "fuente", "temperature_2m_min", "temperature_2m_max", "precipitation_sum", "weather_code"]]'''),
    md("Un pedido inválido es **permanente** y no se reintenta:"),
    code('''try:
    ingesta._pedir(ingesta.URL_ARCHIVE, {"latitude": -33.78, "longitude": 151.23, "daily": "variable_inexistente",
                                         "start_date": "2025-01-01", "end_date": "2025-01-02"})
except ingesta.ErrorPermanente as e:
    print("ErrorPermanente ->", e)'''),
    md("""## 3. Escritura en bronze (Delta Lake en MinIO)
- Solo fechas nuevas → `append`.
- Si el intervalo incluye fechas ya cargadas (backfill / reproceso) → `MERGE` por `fecha` (upsert).

Bronze guarda el dato **crudo** tal como vino (nombres de la API, sin limpiar) + `fuente` e `ingestado_en` para auditoría."""),
    md("## 4. Demo E1: correr dos veces"),
    code('''from deltalake import DeltaTable
def resumen():
    dt = DeltaTable(entorno.BRONZE, storage_options=entorno.STORAGE_OPTIONS)
    df = dt.to_pandas(columns=["fecha", "fuente"])
    return {"version_delta": dt.version(), "filas": len(df), "fechas_unicas": df.fecha.nunique(),
            "desde": df.fecha.min(), "hasta": df.fecha.max()}

print("Corrida 1 ->", ingesta.ingerir(), "filas nuevas"); antes = resumen(); print(antes)'''),
    code('''print("Corrida 2 ->", ingesta.ingerir(), "filas nuevas"); despues = resumen(); print(despues)
assert despues["filas"] == antes["filas"] == despues["fechas_unicas"], "¡duplicados!"
print("✔ Idempotente: la segunda corrida no consultó la API porque no había nada nuevo que pedir")'''),
    md("""**Por qué es idempotente (y no es deduplicación):** la segunda corrida lee el watermark, calcula
`desde = watermark + 1 > hasta` y **no le pide nada al origen**. No traemos lo que ya tenemos.

## 5. Reproceso de un período (lo que usa el backfill de E3)
Pedimos explícitamente una semana ya cargada: se vuelve a extraer y se hace MERGE. Las filas no cambian."""),
    code('''n = ingesta.ingerir(inicio=date(2026, 3, 1), fin=date(2026, 3, 8))
print("reprocesadas:", n); print(resumen())'''),
    code('''# Historial Delta: cada escritura es una versión (auditoría + time travel, ver NB03)
dt = DeltaTable(entorno.BRONZE, storage_options=entorno.STORAGE_OPTIONS)
import pandas as pd
pd.DataFrame(dt.history())[["version", "timestamp", "operation", "operationMetrics"]].head(10)'''),
])

# ---------------------------------------------------------------------------
nb("NB03_semana07_duckdb_delta_lake.ipynb", [
    md(f"""# NB03 · Semana 7 — DuckDB y Delta sobre el lake (exploración)
{CABECERA}

Leemos bronze (Delta) y lo exploramos con SQL en DuckDB. Lo que encontramos acá es lo que limpia la capa silver (E2)."""),
    code(PREP + '''
import duckdb, pandas as pd
from deltalake import DeltaTable
dt = DeltaTable(entorno.BRONZE, storage_options=entorno.STORAGE_OPTIONS)
bronze = dt.to_pandas()
print(bronze.shape, "| versión Delta", dt.version())'''),
    md("## 1. Consultas analíticas con DuckDB (sobre el DataFrame, sin cargarlo a ninguna base)"),
    code('''duckdb.sql("""
select fuente, count(*) dias, min(fecha) desde, max(fecha) hasta,
       round(avg(temperature_2m_mean),1) temp_media, round(sum(precipitation_sum)/count(*)*365,0) lluvia_anual_mm
from bronze group by fuente order by desde""").df()'''),
    md("## 2. Problemas reales encontrados en los datos crudos"),
    code('''duckdb.sql("""
select fuente,
  round(avg(rain_sum),2) prom_rain_sum, round(avg(precipitation_sum),2) prom_precipitation_sum,
  count(*) filter (where precipitation_sum > 0 and precipitation_hours = 0) precip_sin_horas,
  count(*) filter (where wind_direction_10m_dominant = 360) dir_360,
  count(*) filter (where temperature_2m_min > temperature_2m_max) min_mayor_max
from bronze group by fuente""").df()'''),
    md("""- **`rain_sum` no significa lo mismo en las dos fuentes**: en ERA5 es igual a la precipitación; en Historical Forecast excluye los chaparrones (es menor). Silver usa `precipitation_sum`.
- **Días con precipitación > 0 y 0 horas de precipitación** en ERA5: lloviznas por debajo del umbral horario. Se documenta y se usa el criterio "día lluvioso = ≥ 1 mm".
- **Dirección 360° y 0°** = mismo norte → se normaliza."""),
    code('''duckdb.sql("""
select weather_code, count(*) filter (where fuente='archive_era5') era5, count(*) filter (where fuente='historical_forecast') hist_forecast
from bronze group by 1 order by 1""").df()'''),
    md("- **`weather_code` usa vocabularios distintos**: ERA5 nunca codifica niebla (45), chaparrones (80-82) ni tormentas (95-99). Por eso silver los agrupa en categorías y el tablero advierte que 'días de tormenta' antes de 2026 es 0 por la fuente, no por el clima."),
    md("## 3. Time travel sobre Delta\nDelta guarda el historial: podemos leer la tabla como estaba en cualquier versión anterior."),
    code('''hist = pd.DataFrame(dt.history())[["version", "timestamp", "operation"]]
print(hist.head(10))
v0 = DeltaTable(entorno.BRONZE, version=0, storage_options=entorno.STORAGE_OPTIONS).to_pandas()
print("versión 0:", len(v0), "filas | hoy:", len(bronze), "filas")'''),
])

# ---------------------------------------------------------------------------
nb("NB04_semana08_dbt_silver_gold.ipynb", [
    md(f"""# NB04 · Semana 8 — Transformación con dbt: silver y gold (Entrega 2)
{CABECERA}

El proyecto dbt está en `dbt/`. **Silver lee de la tabla Delta de bronze** (source `bronze.clima_diario`
con el plugin `delta` de dbt-duckdb), no de un CSV."""),
    code(PREP + '''
import subprocess
def dbt(*args, env=None):
    r = subprocess.run(["dbt", *args, "--profiles-dir", ".", "--no-use-colors"], cwd=RAIZ / "dbt",
                       capture_output=True, text=True, env={**os.environ, **(env or {})})
    print(r.stdout[-6000:], r.stderr[-2000:])
    return r.returncode'''),
    md("## 1. Modelos y linaje\n```\nsource bronze.clima_diario (Delta) ─► stg_clima_diario ─┬─► fct_clima_mensual (gold)\nseed codigos_wmo ─► stg_codigos_clima ─────────────────┘   feat_clima_diario (gold · features)\n```"),
    code("dbt('ls', '--resource-type', 'model', '--resource-type', 'seed', '--resource-type', 'test')"),
    md("## 2. Build completo: seeds + modelos + tests en orden de dependencias"),
    code("dbt('build')"),
    md("## 3. La gold"),
    code('''import duckdb
con = duckdb.connect(str(RAIZ / "dbt" / "warehouse.duckdb"), read_only=True)
print("silver:", con.sql("select count(*) from stg_clima_diario").fetchone()[0], "filas (días)")
print("gold  :", con.sql("select count(*) from fct_clima_mensual").fetchone()[0], "filas (meses)")
display(con.sql("select mes, temperatura_media_c, anomalia_temperatura_c, lluvia_total_mm, lluvia_referencia_mm, dias_con_lluvia, fuente_del_dato from fct_clima_mensual order by mes desc limit 12").df())
con.close()'''),
    md("""## 4. Demo: romper un dato y ver el test fallar
Copiamos bronze a una tabla **temporal** (no tocamos la real), le metemos un día con la mínima mayor que la máxima
y corremos dbt contra esa copia. `assert_temperaturas_coherentes` tiene que fallar."""),
    code('''import tempfile, pandas as pd
from deltalake import DeltaTable, write_deltalake
tmp = tempfile.mkdtemp()
malo = DeltaTable(entorno.BRONZE, storage_options=entorno.STORAGE_OPTIONS).to_pandas()
i = malo.index[malo.fecha == "2026-03-15"][0]
malo.loc[i, ["temperature_2m_min", "temperature_2m_max"]] = [35.0, 12.0]     # dato malo
write_deltalake(f"{tmp}/bronze", malo)
codigo = dbt("build", "--select", "stg_clima_diario+", env={"BRONZE_PATH": f"{tmp}/bronze", "DUCKDB_PATH": f"{tmp}/prueba.duckdb"})
print("código de salida:", codigo, "(distinto de 0 = el test detectó el problema)")'''),
    md("## 5. Documentación y linaje\n`dbt docs generate` produce el catálogo y el grafo de linaje (`dbt/target/index.html`)."),
    code('''dbt("docs", "generate")
print("Abrir:", RAIZ / "dbt" / "target" / "index.html")'''),
])

# ---------------------------------------------------------------------------
nb("NB05_semana10_airflow_dag.ipynb", [
    md(f"""# NB05 · Semana 10 — El DAG en Airflow (Entrega 3)
{CABECERA}

`dags/pipeline_grupo06.py` encadena cuatro tareas atómicas:

```
preparar_entorno ─► extraer_bronze ─► dbt_build ─► publicar_gold
```
| Tarea | Qué hace | Por qué separada |
|---|---|---|
| `preparar_entorno` | Crea (una vez) un venv propio con deltalake/dbt | El Airflow es compartido y no trae esas librerías; si falla es infraestructura, no datos |
| `extraer_bronze` | Open-Meteo → bronze para el intervalo de la corrida | Depende de una API externa: es la que más reintentos necesita |
| `dbt_build` | silver + gold + tests | Determinística: si un test falla, no se publica |
| `publicar_gold` | DuckDB → PostgreSQL | Toca el warehouse que ve Metabase; reintentable sola |"""),
    code(PREP + '''
print((RAIZ / "dags" / "pipeline_grupo06.py").read_text())'''),
    md("## 1. Validar el DAG antes de desplegar\nSintaxis (`ast`) + estructura: tareas, dependencias y que la extracción use el intervalo y no el reloj."),
    code('''import ast
codigo = (RAIZ / "dags" / "pipeline_grupo06.py").read_text()
arbol = ast.parse(codigo); compile(codigo, "pipeline_grupo06.py", "exec")
print("✔ sintaxis OK")
ids = [kw.value.value for n in ast.walk(arbol) if isinstance(n, ast.Call)
       for kw in n.keywords if kw.arg == "task_id"]
print("tareas:", ids)
assert ids == ["preparar_entorno", "extraer_bronze", "dbt_build", "publicar_gold"]
assert "data_interval_start" in codigo and "data_interval_end" in codigo
print("✔ la extracción recibe el intervalo del contexto")'''),
    md("""## 2. Cómo se ve una corrida
Para la corrida con fecha lógica `2026-03-10 06:00` (ART), Airflow arma el comando:
```
python -m scripts.ingesta --inicio 2026-03-10 --fin 2026-03-11
```
y la ingesta procesa el **10/03 de Sídney**, aunque la ejecutemos hoy. Probamos ese mismo comando acá:"""),
    code('''import subprocess
r = subprocess.run("python -m scripts.ingesta --inicio 2026-03-10 --fin 2026-03-11", shell=True, cwd=RAIZ, capture_output=True, text=True)
print(r.stdout, r.stderr[-1500:])'''),
    md("""## 3. Backfill (cómo dispararlo)
**Un rango, en una sola corrida** — en la UI de Airflow: ▶ → *Trigger DAG w/ config*, con config
```json
{"inicio": "2026-03-01", "fin": "2026-04-01"}
```
La ingesta pide ese rango a la API (un pedido por año) y hace MERGE: no duplica.

**Por fecha lógica** — mismo botón, eligiendo *Logical date* en el pasado. En una corrida **manual** de Airflow 2.9 la fecha
lógica es el **fin** del intervalo: `2026-03-10 06:00` procesa el **9/03**. En las programadas es el inicio.

**¿Y backfillear dos años?** Con corridas diarias serían ~730 corridas × 4 tareas (≈3000 tareas en un Airflow compartido) y 730 pedidos a la API.
Lo correcto es **una corrida con rango** (o pocas, por año): Open-Meteo acepta rangos largos y nuestro `dbt build` es completo, no incremental, así que basta con correrlo una vez al final.

`max_active_runs=1`: dos corridas a la vez escribirían la misma tabla Delta y el mismo archivo DuckDB."""),
])

# ---------------------------------------------------------------------------
nb("NB06_semana14_serving_metabase.ipynb", [
    md(f"""# NB06 · Semana 14 — Serving: gold en PostgreSQL + Metabase (Entrega 4)
{CABECERA}

`scripts/publicar_gold.py` es la última tarea del DAG. Publica `fct_clima_mensual` y `feat_clima_diario`
en el schema `grupo06` de la base `warehouse`."""),
    code(PREP + '''
from sqlalchemy import create_engine, text
from scripts import publicar_gold
engine = create_engine(entorno.pg_url())'''),
    md("## 1. Publicar dos veces y comprobar que no duplica (DELETE + INSERT en una transacción)"),
    code('''def conteos():
    with engine.connect() as c:
        return {t: c.execute(text(f'select count(*) from "{entorno.PG_SCHEMA}"."{t}"')).scalar() for t in publicar_gold.TABLAS}
publicar_gold.main(); a = conteos(); print("1ª:", a)
publicar_gold.main(); b = conteos(); print("2ª:", b)
assert a == b; print("✔ Publicación repetible")'''),
    md("## 2. Consultar la gold (como lo hará Metabase)"),
    code('''import pandas as pd
with engine.connect() as c:
    display(pd.read_sql(text(f"""select mes, temperatura_media_c, anomalia_temperatura_c, lluvia_total_mm, dias_con_lluvia
                                  from {entorno.PG_SCHEMA}.fct_clima_mensual order by mes desc limit 6"""), c))'''),
    md("""## 3. Tablero en Metabase (https://metabase.nexoacademia.org)
Conexión: base **warehouse**, schema **grupo06**, tablas `fct_clima_mensual` y `feat_clima_diario`.
Las preguntas, títulos y configuración de cada gráfico están en `docs/tablero_metabase.md`
(Metabase no se versiona: ese archivo + las capturas en `docs/capturas/` es lo que viaja con el repo).

> Metabase responde preguntas de **negocio** sobre el warehouse; Grafana se usa para **observabilidad** del pipeline."""),
])
