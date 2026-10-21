"""
DAG del proyecto integrador — grupo06 · Clima de Sídney (Open-Meteo).

    preparar_entorno >> extraer_bronze >> dbt_build >> publicar_gold

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
  * Backfill de un rango en UNA corrida: disparar con config
        {"inicio": "2026-03-01", "fin": "2026-04-01"}
"""
from __future__ import annotations

import os

import pendulum
from airflow.models.dag import DAG
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


_cfg = _leer_env(CURSO)
GRUPO = _cfg.get("GRUPO") or _cfg.get("MINIO_BUCKET") or "local"
VENV = _cfg.get("PIPELINE_VENV") or f"/tmp/venv_{GRUPO}"
PY, DBT = f"{VENV}/bin/python", f"{VENV}/bin/dbt"

# Entrar al repo, exportar el .env del grupo y el venv a usar.
_prep = (f'cd "{CURSO}" && set -a && if [ -f ./.env ]; then . ./.env; fi && set +a '
         f'&& export CURSO_DIR="{CURSO}" PIPELINE_VENV="{VENV}" GRUPO="{GRUPO}"')

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
    doc_md=__doc__,
) as dag:

    preparar_entorno = BashOperator(
        task_id="preparar_entorno",
        # Ojo: si el comando terminara en ".sh", Airflow lo tomaría como un archivo de
        # plantilla Jinja (TemplateNotFound). Por eso la ruta va con comillas y sigue texto.
        bash_command=f'{_prep} && bash "{CURSO}/scripts/preparar_entorno.sh" && echo "entorno listo"',
        execution_timeout=pendulum.duration(minutes=20),
    )

    extraer_bronze = BashOperator(
        task_id="extraer_bronze",
        bash_command=f"{_prep} && {PY} -m scripts.ingesta --inicio {INICIO} --fin {FIN}",
        execution_timeout=pendulum.duration(minutes=15),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{_prep} && cd dbt && {DBT} build --profiles-dir . --no-use-colors",
        # dbt es determinístico: si falla un test, reintentar da el mismo resultado.
        # Un único reintento cubre fallas de infraestructura (archivo DuckDB bloqueado).
        retries=1,
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=20),
    )

    publicar_gold = BashOperator(
        task_id="publicar_gold",
        bash_command=f"{_prep} && {PY} -m scripts.publicar_gold",
        retry_delay=pendulum.duration(minutes=2),
        execution_timeout=pendulum.duration(minutes=10),
    )

    # Si dbt_build falla (p. ej. un test en rojo), publicar_gold NO corre (trigger_rule
    # all_success por defecto): el tablero sigue mostrando la última versión válida.
    preparar_entorno >> extraer_bronze >> dbt_build >> publicar_gold
