# Entrega 3 · Orquestación con Airflow e integración continua

DAG: [`dags/pipeline_grupo06.py`](../dags/pipeline_grupo06.py) · CI: [`.github/workflows/ci.yml`](../.github/workflows/ci.yml) · Notebook: [`notebooks/NB05_semana10_airflow_dag.ipynb`](../notebooks/NB05_semana10_airflow_dag.ipynb)

## Cómo levantar Airflow y dónde ver el DAG

**Servidor de la cátedra** (Airflow 2.9.3 compartido por todos los grupos): https://airflow.nexoacademia.org → DAG **`pipeline_grupo06`** (tag `grupo06`).

Airflow monta **solo** `~/work/dags` de cada grupo, como `/opt/airflow/dags/grupo06`. Por eso el repo completo se clona
dentro de esa carpeta (NB00) y el [`.airflowignore`](../.airflowignore) hace que Airflow parsee solo `dags/` (ni notebooks,
ni scripts, ni las carpetas `.ipynb_checkpoints` que crea Jupyter). Para desplegar un cambio: `git push` → ejecutar NB00 (`git pull`).
Airflow lo toma solo a los ~30 s.

**Local**: `make up` → http://localhost:8080 (admin/admin).

## Las tareas y por qué están separadas

```
preparar_entorno ──► extraer_bronze ──► dbt_build ──► publicar_gold          schedule: 0 6 * * * (America/Argentina/Cordoba)
```

| Tarea | Qué hace | Reintentos | Por qué es una tarea aparte |
|---|---|---|---|
| `preparar_entorno` | Crea **una sola vez** un venv propio (`/tmp/venv_grupo06`) con deltalake, duckdb, dbt-duckdb; si ya existe para esa versión de `requirements-pipeline.txt`, tarda < 1 s | 2 × (5, 10 min) | El Airflow compartido no trae esas librerías e instalar en su Python podría romper a otros grupos. Si falla es **infraestructura** (pip/red), no datos: se diagnostica distinto |
| `extraer_bronze` | `python -m scripts.ingesta --inicio <data_interval_start> --fin <data_interval_end>` | 2 × (5, 10 min) | Depende de una API externa: es la falla transitoria más probable. Reintentarla no repite dbt ni la publicación |
| `dbt_build` | silver + gold + features + 34 tests | 1 × 2 min | Es determinística: si un test falla, reintentar da lo mismo. El único reintento cubre fallas de infraestructura (archivo DuckDB bloqueado) |
| `publicar_gold` | DuckDB → PostgreSQL (DELETE + INSERT en una transacción) | 2 × 2 min | Toca el warehouse que ve Metabase. **Solo corre si dbt pasó** (trigger rule `all_success`): un test en rojo no llega al tablero |

Nada pesado en el nivel superior del archivo: solo se lee un `.env` chico. Las tareas son `BashOperator`: cada una es un
proceso propio con el `.env` del grupo, así un Airflow compartido no mezcla credenciales ni módulos entre grupos.
`max_active_runs=1`: dos corridas a la vez competirían por la misma tabla Delta y el mismo archivo DuckDB.

### ¿Por qué 2 reintentos, 5 minutos y backoff?
La falla que esperamos que el reintento resuelva es que **Open-Meteo o MinIO no respondan un rato**. La ingesta ya
reintenta 3 veces en segundos (2 s, 4 s); si sigue fallando, Airflow espera 5 y después 10 minutos: cubre una caída de
~15 minutos sin quedar reintentando para siempre. Más reintentos esconderían un problema real; los errores permanentes
(HTTP 4xx, respuesta malformada) fallan rápido en cada intento y quedan a la vista en el log.

**Reintentar es seguro porque todas las tareas son idempotentes**: la ingesta no trae lo que ya tiene o hace MERGE,
dbt reconstruye las tablas y la publicación reemplaza en una transacción. Sin idempotencia (E1), cada reintento duplicaría datos.

## Backfill

La tarea de extracción **recibe su intervalo del contexto de la corrida** (`data_interval_start` / `data_interval_end`),
nunca del reloj. Una corrida con fecha lógica del 10/03 procesa el 10/03 aunque se ejecute en octubre.

**Opción A — días puntuales, desde la UI** (lo que se muestra en la demo):
1. En `pipeline_grupo06`, botón ▶ → **Trigger DAG w/ config**.
2. En **Logical date** elegir una fecha pasada, p. ej. `2026-03-10 06:00`.
3. **Trigger**. En el log de `extraer_bronze` se ve
   `intervalo pedido: [2026-03-10, 2026-03-11) | a extraer: 2026-03-10 -> 2026-03-10` y `Escritas 1 filas ... (merge)`.
   Las filas totales de bronze no cambian (MERGE), y en el historial de Delta aparece una versión nueva (NB03).

**Opción B — un rango en una sola corrida**: el mismo botón, con esta config:
```json
{"inicio": "2026-03-01", "fin": "2026-04-01"}
```
**Opción C — CLI** (si se tiene acceso al scheduler): `airflow dags backfill pipeline_grupo06 -s 2026-03-01 -e 2026-03-07`.

**¿Qué pasaría si quisiéramos backfillear dos años?** Con corridas diarias serían ~730 corridas × 4 tareas (≈ 2.900
tareas en un Airflow compartido), 730 pedidos a la API y 730 `dbt build` completos, uno por corrida (los modelos no son
incrementales). Lo razonable es la opción B, por año: Open-Meteo devuelve un año en un solo pedido, la ingesta ya parte
por año, y dbt corre una sola vez al final.

## Integración continua

Workflow `CI` en GitHub Actions, en **cada push y cada pull request**. Tres jobs en paralelo (~2-3 min):

| Job | Verifica |
|---|---|
| **Ingesta** | Que `entorno.py` y los scripts compilen + 10 tests de `ci/test_ingesta.py` sin red: primera corrida con las dos APIs, segunda corrida sin pedidos, incremental desde el watermark, backfill sin duplicar, días no publicados, errores transitorios con reintento, errores permanentes sin reintento, respuestas malformadas |
| **dbt** | Crea una tabla Delta local con `ci/bronze_muestra.csv` (78 días **reales** de nuestro bronze, cruzando el cambio de fuente) → `dbt parse` + `dbt compile` (SQL válido) → `dbt build` (modelos + todos los tests de datos) |
| **DAG** | Que el DAG importe en **Airflow 2.9.3** (la versión del servidor) sin errores, con las 4 tareas en ese orden, reintentos entre 1 y 3, `catchup=False`, `max_active_runs=1`, y que la extracción use `data_interval_*` |

**Qué pasa si falla:** la rama `main` está protegida y exige los tres checks en verde para mergear un pull request.
Un cambio que rompe algo queda bloqueado en su PR (ver [defensa.md](defensa.md) para la demo del CI en rojo).
