#!/usr/bin/env bash
# Genera las evidencias de la actividad integradora (scripts + dbt + Airflow).
#
# Correr en una TERMINAL de Jupyter, desde la raíz del grupo:
#     cd ~/work/dags && bash docs/evidencias/generar_evidencias.sh
#
# Deja un .txt por paso en docs/evidencias/. Usa el mismo venv que Airflow. Ningún secreto queda en las salidas:
# los valores de las variables sensibles del .env se reemplazan por "****".
# Escribe en el bronze real y republica la gold: ambas operaciones son idempotentes
# (la segunda corrida de cada script demuestra justamente eso).
set -uo pipefail

RAIZ="$(cd "$(dirname "$0")/../.." && pwd)"
OUT="$RAIZ/docs/evidencias"
cd "$RAIZ"

if [ ! -f .env ]; then
  echo "Falta $RAIZ/.env: generarlo con notebooks/NB00 antes de seguir." >&2
  exit 1
fi
set -a; . ./.env; set +a

# Mismo entorno que Airflow: el Python de Jupyter (/opt/conda) no trae deltalake ni
# pyarrow, así que se usa el venv del pipeline (lo crea una sola vez preparar_entorno.sh).
export PIPELINE_VENV="${PIPELINE_VENV:-/tmp/venv_${GRUPO:-local}}"
echo "== Preparando el venv del pipeline ($PIPELINE_VENV); la primera vez tarda unos minutos"
bash scripts/preparar_entorno.sh || { echo "No se pudo crear el venv del pipeline" >&2; exit 1; }
export PATH="$PIPELINE_VENV/bin:$PATH"
echo "   python: $(command -v python) · dbt: $(command -v dbt)"

# Valores a ocultar en todas las salidas. AWS_ACCESS_KEY_ID no se oculta: en el servidor
# es el nombre del grupo (no es secreto) y ocultarlo tapaba el schema en las salidas.
SECRETOS=()
for v in AWS_SECRET_ACCESS_KEY PGPASSWORD; do
  [ -n "${!v:-}" ] && SECRETOS+=("${!v}")
done
ocultar() {
  local s; local linea
  while IFS= read -r linea; do
    for s in "${SECRETOS[@]}"; do linea="${linea//"$s"/****}"; done
    printf '%s\n' "$linea"
  done
}

# Ejecuta un comando, lo muestra con su salida y su código de salida.
paso() {
  echo "\$ $*"
  bash -c "$*" 2>&1
  echo "→ código de salida: $?"
  echo
}

encabezado() { echo "# $1"; echo "# grupo06 · $(date '+%Y-%m-%d %H:%M %Z') · $(hostname)"; echo; }

echo "== Paso 0: estructura del workspace"
{
  encabezado "Paso 0 - Verificar el workspace"
  paso 'pwd'
  paso "find . -maxdepth 2 -type d -not -path './.git*' -not -path '*__pycache__*' -not -path './.pytest_cache*' | sort"
  paso "find . -maxdepth 3 -type f -not -path './.git/*' -not -path './dbt/target/*' -not -path './dbt/logs/*' -not -path '*__pycache__*' -not -path './.pytest_cache/*' | sort | head -100"
} | ocultar > "$OUT/00_workspace.txt"

echo "== Validaciones antes del despliegue (py_compile)"
{
  encabezado "Validaciones antes del despliegue"
  paso 'python -m py_compile scripts/ingesta.py && echo OK'
  paso 'python -m py_compile scripts/publicar_gold.py && echo OK'
  paso 'python -m py_compile dags/pipeline_grupo06.py && echo OK'
  paso 'grep -nE "AWS_SECRET_ACCESS_KEY *=|PGPASSWORD *=|password *=" scripts/*.py dags/*.py || echo "Sin credenciales escritas en el código"'
} | ocultar > "$OUT/01_validaciones.txt"

echo "== Paso 2: ingesta (dos ejecuciones)"
{
  encabezado "Paso 2 - scripts/ingesta.py: prueba aislada, dos veces seguidas"
  paso 'python -m scripts.ingesta'
  paso 'python -m scripts.ingesta   # segunda vez: no debe consultar la API ni duplicar'
  echo "## Evidencia en Bronze"
  paso "python - <<'PY'
from deltalake import DeltaTable
import entorno
dt = DeltaTable(entorno.BRONZE, storage_options=entorno.STORAGE_OPTIONS)
df = dt.to_pandas(columns=['fecha', 'fuente'])
print('tabla      :', entorno.BRONZE)
print('versión    :', dt.version())
print('filas      :', len(df))
print('fechas únicas:', df.fecha.nunique(), '(filas == fechas únicas -> sin duplicados)')
print('rango      :', df.fecha.min(), '->', df.fecha.max())
print(df.groupby('fuente').size().to_string())
assert len(df) == df.fecha.nunique()
PY"
} | ocultar > "$OUT/02_ingesta.txt"

echo "== Paso 4: dbt (debug, parse, seed, run, test, docs)"
{
  encabezado "Paso 4 - Proyecto dbt (raíz: dbt/)"
  paso 'cd dbt && dbt debug --no-use-colors'
  paso 'cd dbt && dbt parse --no-use-colors'
  paso 'cd dbt && dbt seed --no-use-colors'
  paso 'cd dbt && dbt run --no-use-colors'
  paso 'cd dbt && dbt test --no-use-colors'
  paso 'cd dbt && dbt docs generate --no-use-colors'
} | ocultar > "$OUT/03_dbt.txt"

echo "== Paso 3: publicar_gold (dos ejecuciones)"
{
  encabezado "Paso 3 - scripts/publicar_gold.py: prueba aislada, dos veces seguidas"
  paso 'python -m scripts.publicar_gold'
  paso 'python -m scripts.publicar_gold   # segunda vez: mismos conteos, sin duplicados'
  echo "## Evidencia en PostgreSQL (warehouse)"
  paso "python - <<'PY'
from sqlalchemy import create_engine, text
import entorno
eng = create_engine(entorno.pg_url())
with eng.connect() as c:
    for t, pk in [('fct_clima_mensual', 'mes'), ('feat_clima_diario', 'fecha')]:
        n, u = c.execute(text(f'select count(*), count(distinct {pk}) from {entorno.PG_SCHEMA}.{t}')).one()
        print(f'{entorno.PG_SCHEMA}.{t}: {n} filas, {u} claves distintas', '(sin duplicados)' if n == u else '(¡DUPLICADOS!)')
PY"
} | ocultar > "$OUT/04_publicar_gold.txt"

echo
echo "Listo. Evidencias en docs/evidencias/:"
ls -1 "$OUT"/*.txt
echo
echo "Revisar que no haya secretos antes de commitear:"
for s in "${SECRETOS[@]}"; do grep -l -- "$s" "$OUT"/*.txt && echo "¡OJO! hay un secreto en ese archivo"; done
echo "(si no apareció ningún archivo arriba, está limpio)"
