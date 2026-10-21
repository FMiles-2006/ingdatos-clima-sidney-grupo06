#!/usr/bin/env bash
# Crea (una sola vez) el entorno virtual del pipeline de grupo06 dentro de Airflow.
#
# Por qué: el Airflow del servidor es COMPARTIDO por 10 grupos y su Python no trae
# deltalake ni dbt. Instalar en el Python de Airflow podría romper a otros grupos;
# un venv propio aísla nuestras dependencias. Es idempotente: si el venv ya existe
# para esta versión exacta de requirements, no hace nada (tarda < 1 s).
set -euo pipefail
CURSO="${CURSO_DIR:-$(cd "$(dirname "$0")/.." && pwd)}"
VENV="${PIPELINE_VENV:-/tmp/venv_${GRUPO:-local}}"
REQ="$CURSO/requirements-pipeline.txt"
HUELLA="$(sha256sum "$REQ" | cut -c1-16)"

if [ -x "$VENV/bin/python" ] && [ -f "$VENV/.listo-$HUELLA" ]; then
  echo "venv $VENV listo (requirements $HUELLA). Nada que hacer."
  exit 0
fi

echo "Creando venv $VENV para requirements $HUELLA ..."
rm -rf "$VENV"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet -r "$REQ"
touch "$VENV/.listo-$HUELLA"
"$VENV/bin/python" -c "import deltalake, duckdb, dbt.version; print('OK deltalake', deltalake.__version__, '| duckdb', duckdb.__version__, '| dbt', dbt.version.__version__)"
