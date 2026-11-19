"""
DAG del proyecto integrador — grupo06 · Clima de Sídney (Open-Meteo).

    preparar_entorno >> extraer_bronze >> dbt_run >> dbt_test >> publicar_gold

  * preparar_entorno : venv propio con deltalake + dbt (el Airflow compartido no los trae).
  * extraer_bronze   : python -m scripts.ingesta  (API -> bronze Delta, incremental).
                       Una falla permanente (código 2 o 3) no se reintenta.
  * dbt_run          : dbt seed + dbt run         (bronze -> staging/silver -> marts/gold).
  * dbt_test         : dbt test                   (57 pruebas de calidad).
  * publicar_gold    : python -m scripts.publicar_gold (gold DuckDB -> PostgreSQL).
La DAG solo ORQUESTA: toda la lógica vive en scripts/ y en dbt/.

Diseño para el Airflow COMPARTIDO del servidor de la cátedra:
  * El repositorio vive dentro de la carpeta de DAGs del grupo (la única que Airflow
    monta: /opt/airflow/dags/grupo06). Este archivo está en <repo>/dags/ y ubica el
    repo con __file__. El .airflowignore de la raíz hace que Airflow solo parsee dags/.
  * Cada tarea es un PROCESO propio (BashOperator) que entra al repo, carga el .env del
    grupo y usa un venv propio: nada se mezcla con los otros grupos.
  * Nivel superior liviano: acá solo se lee un .env chico. Nada de red ni de bases:
    Airflow re-parsea este archivo cada pocos segundos.

Intervalo y backfill:
  * La extracción recibe el intervalo de la corrida (data_interval_start/end), nunca el
    reloj. Una corrida con fecha lógica 2026-03-01 procesa el 2026-03-01 aunque se
    ejecute hoy.
  * Backfill de un rango en UNA corrida: botón ▶ (Trigger DAG) → formulario con los
    campos "inicio" y "fin" (o el JSON {"inicio": "2026-03-01", "fin": "2026-04-01"}).
    Vacíos = corrida normal. "fin" no se incluye: con fin 2026-03-08 el último día es el 7.
"""
from __future__ import annotations

import os
import re

import pendulum
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

HERE = os.path.dirname(os.path.abspath(__file__))
CURSO = os.environ.get("CURSO_DIR") or os.path.dirname(HERE)   # raíz del repo


def _leer_env(base: str) -> dict:
    cfg = {}
    ruta = os.path.join(base, ".env")
    if os.path.exists(ruta):
        for linea in open(ruta, encoding="utf-8"):
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                k, v = linea.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg


class BashSinReintentarPermanentes(BashOperator):
    """BashOperator que NO reintenta las fallas permanentes.

    scripts/ingesta.py sale con 1 si la falla es transitoria (API caída, timeout, 429/5xx),
    con 2 si es permanente (HTTP 4xx, respuesta malformada) y con 3 si falta configuración.
    Reintentar una falla permanente no sirve: con 2 o 3 la tarea falla de inmediato
    (AirflowFailException) en lugar de esperar 5 + 10 minutos de reintentos inútiles.
    """

    CODIGOS_PERMANENTES = (2, 3)

    def execute(self, context):
        try:
            return super().execute(context)
        except AirflowException as e:
            m = re.search(r"exit code (\d+)", str(e))
            if m and int(m.group(1)) in self.CODIGOS_PERMANENTES:
                raise AirflowFailException(
                    f"Falla permanente (código {m.group(1)}): no se reintenta, hay que revisar el log") from e
            raise


_cfg = _leer_env(CURSO)
GRUPO = _cfg.get("GRUPO") or _cfg.get("MINIO_BUCKET") or "local"
VENV = _cfg.get("PIPELINE_VENV") or f"/tmp/venv_{GRUPO}"
PY, DBT = f"{VENV}/bin/python", f"{VENV}/bin/dbt"

# Artefactos de cómputo de dbt (archivo DuckDB, target/, logs/) en /tmp del worker.
# Airflow corre con otro usuario que Jupyter y no puede escribir en la carpeta del repo.
# No hace falta que persistan: `dbt run` reconstruye staging/marts enteras desde bronze en
# cada corrida; el estado durable vive en bronze (Delta/MinIO) y en gold (PostgreSQL).
TRABAJO = f"/tmp/dbt_{GRUPO}"

# Entrar al repo, exportar el .env del grupo, el venv y dónde escribe dbt.
_prep = (f'cd "{CURSO}" && set -a && if [ -f ./.env ]; then . ./.env; fi && set +a '
         f'&& mkdir -p "{TRABAJO}" '
         f'&& export CURSO_DIR="{CURSO}" PIPELINE_VENV="{VENV}" GRUPO="{GRUPO}" '
         f'DUCKDB_PATH="{TRABAJO}/warehouse.duckdb" DBT_TARGET_PATH="{TRABAJO}/target" DBT_LOG_PATH="{TRABAJO}/logs"')

# Intervalo de la corrida: viene del contexto. Si se dispara con config {inicio, fin}, manda la config.
INICIO = "{{ dag_run.conf.get('inicio') or (data_interval_start | ds) }}"
FIN = "{{ dag_run.conf.get('fin') or (data_interval_end | ds) }}"

default_args = {
    "owner": GRUPO,
    # 2 reintentos con espera creciente (5 min, 10 min): cubren una caída corta de la API
    # o de MinIO. Más reintentos esconderían un problema real. Reintentar es seguro
    # porque todas las tareas son idempotentes.
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": pendulum.duration(minutes=30),
}

with DAG(
    dag_id=f"pipeline_{GRUPO}",
    description="Open-Meteo (Sídney) -> bronze Delta -> dbt silver/gold -> PostgreSQL -> Metabase",
    # 06:00 de Argentina: el día anterior de Sídney (UTC+10/+11) ya cerró hace ~20 h y
    # la Historical Forecast API ya lo tiene completo.
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Argentina/Cordoba"),
    catchup=False,           # el histórico 2022-2025 lo trae la 1ª corrida (watermark vacío)
    max_active_runs=1,       # dos corridas a la vez competirían por la tabla Delta y el archivo DuckDB
    default_args=default_args,
    tags=["proyecto-integrador", GRUPO, "open-meteo"],
    # Declarar los parámetros hace que el botón ▶ abra un formulario para el backfill.
    # Sin params, Airflow 2.9 dispara la corrida directo y no deja cargar el intervalo.
    params={
        "inicio": Param(None, type=["null", "string"], format="date", title="Inicio del backfill",
                        description="Primer día a reprocesar (AAAA-MM-DD). Vacío = corrida normal."),
        "fin": Param(None, type=["null", "string"], format="date", title="Fin del backfill (no incluido)",
                     description="Día siguiente al último a reprocesar. Vacío = corrida normal."),
    },
    doc_md=__doc__,
) as dag:

    preparar_entorno = BashOperator(
        task_id="preparar_entorno",
        # Ojo: si el comando terminara en ".sh", Airflow lo tomaría como un archivo de
        # plantilla Jinja (TemplateNotFound). Por eso la ruta va con comillas y sigue texto.
        bash_command=f'{_prep} && bash "{CURSO}/scripts/preparar_entorno.sh" && echo "entorno listo"',
        execution_timeout=pendulum.duration(minutes=20),
    )

    extraer_bronze = BashSinReintentarPermanentes(
        task_id="extraer_bronze",
        bash_command=f"{_prep} && {PY} -m scripts.ingesta --inicio {INICIO} --fin {FIN}",
        execution_timeout=pendulum.duration(minutes=15),
    )

    _dbt = f"{_prep} && cd dbt && {DBT}"   # dbt encuentra dbt_project.yml y profiles.yml en dbt/

    dbt_run = BashOperator(
        task_id="dbt_run",
        # seed: catálogos de referencia (códigos WMO y fases de El Niño). run: staging + marts.
        bash_command=f"{_dbt} seed --profiles-dir . --no-use-colors && {DBT} run --profiles-dir . --no-use-colors",
        # dbt es determinístico: un único reintento cubre fallas de infraestructura
        # (archivo DuckDB bloqueado, MinIO caído un instante).
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=20),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{_dbt} test --profiles-dir . --no-use-colors",
        # Si un test falla, reintentar da lo mismo: el reintento solo cubre infraestructura.
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=10),
    )

    publicar_gold = BashOperator(
        task_id="publicar_gold",
        bash_command=f"{_prep} && {PY} -m scripts.publicar_gold",
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=10),
    )

    # Si dbt_test falla (un test en rojo), publicar_gold NO corre (trigger_rule
    # all_success por defecto): el tablero sigue mostrando la última versión válida.
    preparar_entorno >> extraer_bronze >> dbt_run >> dbt_test >> publicar_gold
