#!/usr/bin/env bash
# Corre el MISMO pipeline que Airflow ejecuta en el servidor, pero contra destinos locales:
#   Open-Meteo -> bronze (Delta en local/lake/) -> dbt seed/run/test -> PostgreSQL local
# Es idempotente: la primera vez trae 2022-01-01 -> ayer; las siguientes, solo lo que falta
# (watermark). Lo usa el servicio "pipeline" de local/docker-compose.yml.
set -euo pipefail
cd /app
echo "== 1/4 Dependencias del pipeline (con caché de pip)"
pip install --quiet --disable-pip-version-check -r requirements-pipeline.txt
echo "== 2/4 Ingesta: Open-Meteo -> bronze ($BRONZE_PATH)"
python -m scripts.ingesta
echo "== 3/4 dbt: seed + run + test"
cd dbt
dbt seed --profiles-dir . --no-use-colors
dbt run  --profiles-dir . --no-use-colors
dbt test --profiles-dir . --no-use-colors
cd ..
echo "== 4/4 Publicar gold en PostgreSQL local ($PGHOST/$PGDATABASE, schema $PGSCHEMA)"
python -m scripts.publicar_gold
echo "== Pipeline local OK"
