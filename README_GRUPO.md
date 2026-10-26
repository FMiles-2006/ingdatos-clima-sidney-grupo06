# README_GRUPO · Actividad integradora (scripts + dbt + Airflow)

| | |
|---|---|
| **Grupo** | grupo06 |
| **Integrantes** | Bautista Verna · Francisco Miles · Tomás Lizazu · Martín Masuelli · Juan Ancarani · Felipe Dal Lago |
| **API / fuente de datos** | Open-Meteo: Archive API (ERA5) + Historical Forecast API · clima diario de Sídney (−33.7786, 151.2314) |
| **Fecha de entrega** | 27/10/2026 |
| **Repositorio** | https://github.com/FMiles-2006/ingdatos-clima-sidney-grupo06 |
| **DAG** | `pipeline_grupo06` en https://airflow.nexoacademia.org |

El pipeline corre sin intervención manual: Airflow ejecuta procesos independientes (scripts de Python y
comandos de dbt) que leen su configuración del `.env` del grupo. Los notebooks quedan como evidencia del
análisis; ninguna tarea de Airflow depende de ejecutar sus celdas.

```
Open-Meteo ──► scripts/ingesta.py ──► bronze (Delta en MinIO) ──► dbt run: staging ─► marts ──► dbt test ──► scripts/publicar_gold.py ──► PostgreSQL ──► Metabase
                    (Tarea 1)                                          (Tarea 2)              (Tarea 3)            (Tarea 4)
```

---

## 1. Estructura del workspace

En el servidor la raíz del grupo es **`~/work/dags`** (Airflow solo monta esa carpeta; el `.airflowignore`
hace que solo parsee `dags/`).

```
~/work/dags/                       (raíz del grupo = raíz del repo)
├── .env                           credenciales y parámetros (NO se versiona; lo genera NB00)
├── .env.example                   nombres de las variables, sin secretos
├── entorno.py                     lee el .env y expone rutas y conexiones
├── notebooks/
│   ├── NB02_semana04_extraccion_bronze.ipynb
│   ├── NB03_semana07_duckdb_delta_lake.ipynb
│   └── NB04_semana08_dbt_silver_gold.ipynb      (+ NB00, NB01, NB05, NB06)
├── scripts/
│   ├── __init__.py
│   ├── ingesta.py                 Tarea 1
│   └── publicar_gold.py           Tarea 4
├── dbt/                           ÚNICA raíz del proyecto dbt
│   ├── dbt_project.yml
│   ├── profiles.yml               sin secretos: todo con env_var() (ver nota)
│   ├── models/
│   │   ├── sources.yml            source bronze.clima_diario (tabla Delta)
│   │   ├── staging/               stg_clima_diario, stg_codigos_clima   (capa silver)
│   │   └── marts/                 fct_clima_mensual, feat_clima_diario  (capa gold)
│   ├── seeds/codigos_wmo.csv
│   ├── macros/test_valor_en_rango.sql
│   └── tests/                     4 pruebas singulares
├── dags/
│   └── pipeline_grupo06.py
├── ci/                            tests automáticos (GitHub Actions)
└── docs/evidencias/               salidas de terminal de esta actividad
```

**Nota sobre `profiles.yml`.** La consigna pide `profiles.yml.example` porque un perfil suele tener contraseñas.
El nuestro no tiene ninguna: el destino es un archivo DuckDB y las credenciales de MinIO se leen con `env_var()`
desde el `.env`. Por eso se versiona tal cual y vive dentro de `dbt/`, así `cd dbt && dbt debug` lo encuentra sin
`--profiles-dir` ni configuración extra.

---

## 2. Variables requeridas (sin secretos)

Se leen del `.env` (o de variables de entorno ya presentes). En el servidor el `.env` lo genera `notebooks/NB00`
a partir de las variables que inyecta JupyterHub; nadie escribe claves a mano.

| Variable | Para qué | Obligatoria |
|---|---|---|
| `GRUPO` | identifica al grupo: `dag_id`, venv y carpetas temporales | sí |
| `MINIO_BUCKET` | bucket del grupo; bronze = `s3://<bucket>/bronze/clima_sidney` | sí |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | credenciales de MinIO | sí (si bronze está en S3) |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ALLOW_HTTP` | endpoint de MinIO | sí (endpoint) |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | warehouse PostgreSQL (`warehouse`) | sí (publicación) |
| `PGSCHEMA` | schema del grupo (`grupo06`) | no (por defecto = `GRUPO`) |
| `BRONZE_PATH`, `DUCKDB_PATH` | sobrescriben rutas (CI, pruebas) | no |

Si falta una variable obligatoria, los scripts terminan con **código 3** y un mensaje que nombra la variable
(sin mostrar ningún valor):

```
ERROR CONFIGURACIÓN INVÁLIDA: Faltan variables obligatorias en el .env o el entorno: AWS_SECRET_ACCESS_KEY. ...
```

---

## 3. Paso 1 · Análisis del notebook de extracción (NB02)

### Decisiones del grupo

| Elemento | Decisión del grupo |
|---|---|
| **Endpoint y parámetros** | Hasta 2025-12-31: `https://archive-api.open-meteo.com/v1/archive` (reanálisis ERA5). Desde 2026-01-01: `https://historical-forecast-api.open-meteo.com/v1/forecast` (ERA5 llega con días de demora). Parámetros: `latitude=-33.7786`, `longitude=151.2314`, `timezone=Australia/Sydney`, `start_date`, `end_date`, `daily=` 12 variables (temperaturas máx/mín/media, sensación térmica, precipitación, lluvia, horas de precipitación, viento, ráfagas, dirección, radiación, `weather_code`). Un pedido por año calendario y por API. |
| **Formato de respuesta** | JSON. Bloque `daily` con listas paralelas (`time` + una lista por variable) y `daily_units` con las unidades. Se valida: código HTTP, ausencia de `error`, presencia de `daily` y de las 12 variables, largos iguales, respuesta no vacía, unidad `°C`, fechas válidas, sin repetidas y dentro del rango pedido. |
| **Clave temporal / watermark** | `fecha` (día local de Sídney, `YYYY-MM-DD`). Watermark = `max(fecha)` en bronze, derivado de los datos (no de un archivo aparte). Cada corrida pide desde `watermark + 1`. |
| **Destino Bronze** | Tabla Delta Lake en MinIO: `s3://grupo06/bronze/clima_sidney`. Columnas crudas + `fuente`, `latitud`, `longitud`, `ingestado_en`. |
| **Variables del .env** | Ver sección 2: `MINIO_BUCKET`, `AWS_*` (MinIO) y `PG*` (warehouse). La API de Open-Meteo no requiere clave. |
| **Criterio para evitar duplicados** | 1) Incremental: solo se piden fechas posteriores al watermark; si no hay nada nuevo, no se consulta la API. 2) Si el intervalo incluye fechas ya cargadas (backfill), se escribe con `MERGE` por `fecha` (upsert), no con `append`. |

### Responsabilidades de cada celda de NB02 → función del script

| Responsabilidad | En NB02 | En `scripts/ingesta.py` |
|---|---|---|
| Lectura de configuración y credenciales | celda de preparación (`import entorno`) | `cargar_configuracion()` + `entorno.py` |
| Watermark / criterio incremental | sección 1 (`leer_watermark`) | `leer_watermark()` usada por `cargar_configuracion()` |
| Llamada a la API | sección 2 (`extraer`, muestra) | `extraer_datos()` → `_pedir()` |
| Validación de la respuesta | sección 2 (pedido inválido de ejemplo) | `_validar_respuesta()` dentro de `extraer_datos()` |
| Normalización de registros | sección 2 (tabla de muestra) | `transformar_datos()` |
| Escritura en Bronze | secciones 3 y 4 | `guardar_bronze()` |
| Mensajes de control y errores | salidas impresas | `logging` + `ErrorTransitorio` / `ErrorPermanente` / `ErrorConfiguracion` y códigos de salida |

---

## 4. Scripts

### `scripts/ingesta.py` (Tarea 1)

```
ingerir_incremental()
  ├─ cargar_configuracion()  valida variables, lee watermark, calcula [desde, hasta]
  ├─ extraer_datos()         pide a la API solo ese rango, valida cada respuesta
  ├─ transformar_datos()     time→fecha, tipos float64, descarta días aún no publicados, agrega columnas de control
  └─ guardar_bronze()        append (fechas nuevas) o MERGE por fecha (reproceso)
```

| Control mínimo | Cómo se cumple |
|---|---|
| Sin credenciales en el código | Todo sale de `entorno.py` / `.env`. Lo verifica `ci/test_dag.py` y `docs/evidencias/01_validaciones.txt` |
| Falla con mensaje claro si falta una variable | `ErrorConfiguracion` → código de salida 3 (test `test_falta_variable_obligatoria_...`) |
| Valida el código de estado / la respuesta | `_pedir()`: 200 → JSON; 429/5xx → reintento con backoff (2 s, 4 s); otros 4xx → error permanente sin reintento. `_validar_respuesta()` revisa la forma |
| Controla respuestas vacías | respuesta sin días → `ErrorPermanente`; días con todas las mediciones nulas (aún no publicados) no se escriben y el watermark no avanza |
| Informa obtenidos y escritos | última línea del log: `RESUMEN: obtenidas de la API=N | escritas en bronze=M | modo=append/merge/sin_cambios` |
| Dos ejecuciones sin duplicar | la segunda ve `desde > hasta` y no consulta la API; el backfill usa `MERGE` |

Códigos de salida: `0` ok · `1` falla transitoria (Airflow reintenta) · `2` falla permanente · `3` configuración inválida.

### `scripts/publicar_gold.py` (Tarea 4)

`publicar_gold()` = `leer_gold()` (lee las tablas de `models/marts` desde el DuckDB de dbt) →
`publicar_resultado()` (las copia a PostgreSQL, base `warehouse`, schema `grupo06`).

**Por qué hace falta esta tarea** (no es una tarea vacía): dbt calcula la gold en un archivo DuckDB que el pipeline
reconstruye en cada corrida y que vive en `/tmp` del worker. Metabase no puede leer ese archivo; se conecta a
PostgreSQL. La publicación es repetible: `DELETE + INSERT` dentro de una transacción por tabla (nunca hay una tabla
a medio cargar ni duplicados) y verifica que la cantidad final de filas sea la esperada. Si una gold viene vacía, no
publica, para no vaciar el tablero.

---

## 5. Proyecto dbt

Raíz única: **`dbt/`** (`dbt_project.yml` + `profiles.yml`). Motor: DuckDB con el plugin `delta`, que lee bronze
directamente desde la tabla Delta (no se usa ningún CSV de datos; el único seed es el catálogo de códigos WMO).

| Capa | Modelo | Grano | Qué hace |
|---|---|---|---|
| source | `bronze.clima_diario` | 1 fila por día | tabla Delta cruda |
| seed | `codigos_wmo` | 1 fila por código | catálogo estático de códigos de estado del tiempo |
| **staging** (silver) | `stg_clima_diario` | 1 fila por día | tipa, renombra al español, deduplica, descarta `rain_sum` (mide distinto en cada API), normaliza la dirección del viento, agrega estación y `es_dia_lluvioso` |
| **staging** (silver) | `stg_codigos_clima` | 1 fila por código | descripción en español y categoría comparable entre APIs |
| **marts** (gold) | `fct_clima_mensual` | 1 fila por mes | métricas del mes y anomalías respecto del promedio 2022-2025 de ese mes. Es la salida que consume Metabase |
| **marts** (gold) | `feat_clima_diario` | 1 fila por día | features (promedios móviles 7/30 días, lluvia acumulada, días desde la última lluvia…) sin fuga temporal |

**Diferencia entre staging y marts.** Staging deja cada dato *limpio y confiable* al mismo grano que la fuente
(un día), sin decidir para qué se va a usar. Marts responde preguntas concretas de consumo: agrega (mes),
compara contra una referencia o arma features para un modelo. Si cambia una pregunta de negocio, cambia un mart,
no staging.

### Pruebas (34)

| Tipo | Cantidad | Dónde |
|---|---|---|
| `not_null` | 12 | claves y fechas obligatorias, temperaturas, precipitación, fuente |
| `unique` | 4 | `fecha` en `stg_clima_diario` y `feat_clima_diario`, `mes` en `fct_clima_mensual`, `codigo_wmo` en el catálogo. Es correcto: el dataset es **diario** y la ingesta garantiza una fila por día |
| `relationships` | 1 | `stg_clima_diario.codigo_wmo` → `stg_codigos_clima.codigo_wmo` |
| `accepted_values` | 3 | `estacion`, `fuente`, `categoria` |
| `valor_en_rango` (genérica propia) | 10 | rangos físicos de temperatura, lluvia, viento, dirección y features |
| singulares (`dbt/tests/`) | 4 | mínima ≤ máxima; serie sin huecos; días por mes ≤ días del calendario; features sin fuga temporal |

---

## 6. DAG `pipeline_grupo06`

```
preparar_entorno ──► extraer_bronze ──► dbt_run ──► dbt_test ──► publicar_gold      (todos los días 06:00 ART)
```

| Tarea | Comando (BashOperator) |
|---|---|
| `preparar_entorno` | `bash scripts/preparar_entorno.sh`: crea una sola vez el venv `/tmp/venv_grupo06` con deltalake y dbt (el Airflow compartido no los trae). Idempotente: si ya existe, tarda < 1 s |
| `extraer_bronze` | `python -m scripts.ingesta --inicio {{ data_interval_start }} --fin {{ data_interval_end }}` |
| `dbt_run` | `cd dbt && dbt seed && dbt run` |
| `dbt_test` | `cd dbt && dbt test` |
| `publicar_gold` | `python -m scripts.publicar_gold` |

- Cada tarea entra a la raíz del grupo (`cd ~/work/dags`), exporta el `.env` del grupo y usa el venv del grupo.
- `dag_id = pipeline_grupo06`: no colisiona con otros grupos; no importa módulos de otros grupos.
- La DAG **no tiene lógica de extracción ni de transformación** y no contiene credenciales (lo verifica el CI).
- La extracción usa el intervalo de la corrida, nunca el reloj: así funciona el backfill
  (`Trigger DAG w/ config` → `{"inicio": "2026-03-01", "fin": "2026-03-08"}`).
- Si `dbt_test` falla, `publicar_gold` no corre: el tablero sigue con la última versión válida.

---

## 7. Comandos de prueba

Desde una terminal de Jupyter, en la raíz del grupo:

```bash
cd ~/work/dags
set -a; . ./.env; set +a          # solo para dbt; los scripts ya leen el .env solos

# Validaciones antes del despliegue
python -m py_compile scripts/ingesta.py
python -m py_compile scripts/publicar_gold.py
python -m py_compile dags/pipeline_grupo06.py

# Tarea 1 · ingesta (dos veces: la segunda no debe escribir nada)
python -m scripts.ingesta ; echo $?
python -m scripts.ingesta ; echo $?

# Tareas 2 y 3 · dbt
cd dbt
dbt debug
dbt parse
dbt seed
dbt run
dbt test
dbt docs generate
cd ..

# Tarea 4 · publicación (dos veces: mismos conteos)
python -m scripts.publicar_gold ; echo $?
python -m scripts.publicar_gold ; echo $?
```

Todo lo anterior, con las salidas guardadas en `docs/evidencias/` y los secretos ocultos:

```bash
bash docs/evidencias/generar_evidencias.sh
```

Tests automáticos (también corren en GitHub Actions en cada push y PR):

```bash
pytest -q ci/test_ingesta.py     # ingesta sin red: incremental, idempotencia, errores, variables faltantes
```

---

## 8. Qué código se extrajo de cada notebook y qué se descartó

| Notebook | Pasó a código reutilizable | Quedó solo en el notebook (exploratorio / demostración) |
|---|---|---|
| **NB02** extracción | Watermark, pedidos con reintentos y clasificación de errores, validación de la respuesta, normalización, escritura append/MERGE → `scripts/ingesta.py`, separado en funciones por responsabilidad | Vista previa de una muestra de días, pedido inválido de ejemplo para mostrar el error permanente, corridas repetidas con `assert` y `resumen()`, demo de backfill de una semana, historial de versiones Delta |
| **NB03** DuckDB + Delta | Las conclusiones, no el código: descartar `rain_sum` (mide distinto en cada API) y agrupar `weather_code` en categorías comparables → `stg_clima_diario` y `stg_codigos_clima` | Consultas de exploración por fuente, comparación de vocabularios de códigos entre APIs, time travel sobre Delta |
| **NB04** dbt | Nada que copiar: los modelos, pruebas y configuración ya viven en `dbt/`. El notebook solo invoca dbt | Lectura de la gold con DuckDB para mostrarla, demo de un dato malo en una copia temporal de bronze para ver fallar un test |
| **NB06** serving | Publicación en PostgreSQL → `scripts/publicar_gold.py` | Doble publicación con comparación de conteos, consulta de muestra de la gold |

Además se descartó: rangos de fechas escritos a mano, `print`/`display` de inspección, y cualquier valor de
conexión fijo (todo pasó al `.env`).

---

## 9. Diagnóstico ante fallas (Paso 6)

| Falla observada | Verificación directa | Corrección |
|---|---|---|
| No importa `scripts.ingesta` | `pwd` debe ser `~/work/dags`; existe `scripts/__init__.py` | correr con `python -m` desde la raíz del grupo (la DAG hace `cd` a la raíz) |
| Falta una variable | el log dice cuál (código 3); `grep -c NOMBRE .env` sin mostrar valores | regenerar el `.env` con NB00 |
| dbt no encuentra el proyecto | `cd dbt && dbt debug` | la raíz dbt es `dbt/`; la DAG usa `--profiles-dir .` |
| Modelo o test falla | `dbt run --select <modelo>` / `dbt test --select <modelo>` | corregir SQL, source o prueba |
| Airflow no muestra la DAG | `python -m py_compile dags/pipeline_grupo06.py`; errores de importación en la UI | corregir sintaxis, ruta o `dag_id` |

---

## 10. Evidencias

Hay dos fuentes de evidencia, y se complementan:

- **Ejecución manual (terminal de Jupyter):** `bash docs/evidencias/generar_evidencias.sh` corre cada script y
  cada comando de dbt dos veces donde corresponde, y guarda la salida en `docs/evidencias/*.txt` con los secretos
  ocultos. Usa el mismo venv que Airflow (`/tmp/venv_grupo06`), porque el Python de la terminal de Jupyter
  (`/opt/conda`) no trae `deltalake` ni `pyarrow`.
- **Ejecución orquestada (Airflow):** el log de cada tarea de `pipeline_grupo06` muestra la salida completa del
  script o del comando de dbt que ejecutó, ya con las fechas de la corrida.

| Entregable de la consigna | Archivo |
|---|---|
| Estructura inicial del workspace (Paso 0) | `docs/evidencias/00_workspace.txt` |
| `py_compile` de los 3 archivos y búsqueda de credenciales en el código | `docs/evidencias/01_validaciones.txt` |
| Ejecución manual de `ingesta.py` (dos veces) + evidencia en Bronze | `docs/evidencias/02_ingesta.txt` |
| `dbt debug`, `parse`, `seed`, `run`, `test`, `docs generate` | `docs/evidencias/03_dbt.txt` |
| Ejecución manual de `publicar_gold.py` (dos veces) + evidencia en PostgreSQL | `docs/evidencias/04_publicar_gold.txt` |
| Linaje de dbt (staging → marts) | `docs/capturas/linaje_dbt.png` |
| DAG con las 5 tareas | `docs/capturas/airflow_dag_graph.png` |
| Ejecución completa de la DAG en verde (corrida programada `scheduled__2026-10-25T09:00`, se ejecutó el 26/10 a las 06:00 ART) | `docs/capturas/airflow_dag_grid.png` · `docs/capturas/airflow_dag_gantt.png` |
| Log de cada tarea de esa corrida | `docs/capturas/airflow_log_<tarea>.png` (`preparar_entorno`, `extraer_bronze`, `dbt_run`, `dbt_test`, `publicar_gold`) |
| Manejo de errores en una falla real | `docs/capturas/airflow_log_extraer_bronze_falla_red.png` |

### Incidente del 23/10/2026: corte de red en el servidor

La tarde del 23/10 el servidor de la cátedra perdió la resolución de nombres, tanto en Jupyter como en el worker de
Airflow: no se podía llegar ni a `pypi.org` ni a `open-meteo.com`. La corrida automática de las 06:00 de ese día
había terminado bien. La corrida manual de las 19:41 UTC quedó como evidencia de que el manejo de errores funciona
como lo diseñamos:

```
INFO  Destino bronze: s3://grupo06/bronze/clima_sidney | watermark: 2026-10-22 | intervalo pedido: [2026-10-22, 2026-10-23) ...
INFO  Pidiendo 2026-10-22 -> 2026-10-22 a historical_forecast
WARNING Falla transitoria (sin respuesta (ConnectionError)). Reintento 1/2 en 2s
WARNING Falla transitoria (sin respuesta (ConnectionError)). Reintento 2/2 en 4s
ERROR FALLA TRANSITORIA (Airflow reintentará la tarea): https://historical-forecast-api.open-meteo.com/v1/forecast sigue fallando tras 3 intentos
Command exited with return code 1          → Airflow: up_for_retry
```

- La lectura de bronze en MinIO funcionó (leyó el watermark): la falla estaba solo en la salida a internet.
- El script la clasificó como **transitoria**, reintentó con espera creciente y terminó con **código 1**.
- Airflow puso la tarea en `up_for_retry`. Las tareas siguientes no corrieron, así que el tablero conservó la última
  versión válida y no se publicaron datos incompletos.
- No hizo falta relanzar nada a mano. Cuando volvió la red, las corridas automáticas del 24, 25 y 26/10 terminaron
  en verde, y la estrategia incremental recuperó sola el día que había quedado pendiente: cada corrida pide desde
  `watermark + 1`, no solo el día de su intervalo. La prueba `assert_serie_sin_huecos` pasó en esas corridas, o sea
  que la serie quedó sin días faltantes. No hubo que cambiar el código.

## 11. Preguntas de cierre

**1. ¿Por qué Airflow no debería depender de la ejecución manual de un notebook?**
Porque un notebook depende del estado de la sesión: el orden en que se corrieron las celdas, variables que quedaron
en memoria y valores probados a mano. Eso no es reproducible y Airflow no lo puede controlar. Además, de una celda
no sale un código de salida claro ni un log por paso, así que no se puede reintentar ni diagnosticar solo la parte
que falló. Un script se ejecuta igual cada vez, recibe sus parámetros (el intervalo de la corrida) y devuelve 0 o un
código de error que Airflow interpreta.

**2. ¿Qué responsabilidad tiene `scripts/ingesta.py` y qué responsabilidad tiene la DAG?**
`ingesta.py` sabe *cómo* traer los datos: valida la configuración, calcula el rango a partir del watermark, consulta
la API, valida la respuesta, normaliza y escribe en bronze sin duplicar. La DAG decide *cuándo y en qué orden*:
programa la corrida diaria, le pasa al script el intervalo de la corrida, encadena ingesta → dbt → publicación,
reintenta las fallas transitorias y frena la publicación si un test falla. La DAG no tiene lógica de datos.

**3. ¿Qué parte del procesamiento corresponde a dbt y cuál no?**
A dbt le corresponde todo lo que transforma datos que ya están en el lake con SQL: limpieza y tipado (staging),
agregaciones, anomalías y features (marts), y las pruebas de calidad. No le corresponde hablar con la API ni escribir
bronze (eso es `ingesta.py`, porque necesita pedidos HTTP, reintentos y el watermark), ni copiar la gold al warehouse
de serving (eso es `publicar_gold.py`), ni la programación de las corridas (Airflow).

**4. ¿Cómo garantiza el grupo que una segunda ejecución no duplica información?**
En tres niveles. En bronze, la ingesta pide solo desde `watermark + 1`: si ya está al día, ni consulta la API; y si
una corrida reprocesa fechas ya cargadas (backfill), escribe con `MERGE` por `fecha`, que actualiza en vez de
agregar. En dbt, los modelos se reconstruyen completos desde bronze y la prueba `unique` sobre `fecha` y `mes` lo
verifica en cada corrida. En PostgreSQL, `publicar_gold.py` hace `DELETE + INSERT` en una transacción y la tabla
tiene clave primaria. Las evidencias muestran dos ejecuciones seguidas con los mismos conteos.

**5. ¿Qué prueba de dbt resultó más importante para los datos del grupo y por qué?**
`assert_serie_sin_huecos`. Trabajamos con una serie diaria armada con dos APIs y una carga incremental: si un día
quedara sin cargar (por ejemplo, porque el origen todavía no lo había publicado), el watermark podría saltarlo y el
hueco no se vería a simple vista. Ese hueco rompería los promedios mensuales y las ventanas móviles de 7 y 30 días de
`feat_clima_diario`. La prueba detecta cualquier día faltante entre la primera y la última fecha. Le sigue en
importancia `assert_temperaturas_coherentes` (mínima ≤ máxima), que es la que usamos en NB04 para mostrar cómo un
dato malo frena la publicación.

**6. ¿Qué información aporta el log de una tarea de Airflow cuando el pipeline falla?**
Dice en qué tarea falló (ingesta, construcción, calidad o publicación) y con qué código de salida, y muestra el
comando exacto que se ejecutó, ya con las fechas de la corrida, para poder reproducirlo en la terminal. También
muestra el mensaje del propio script, por ejemplo qué variable falta, qué HTTP devolvió la API y si el error es
transitorio o permanente, o qué modelo o test de dbt falló y con cuántas filas. Por último, registra en qué intento
falló y si Airflow va a reintentar.

---

## 12. Lista de control final

- [x] Los scripts funcionan fuera de Jupyter: Airflow los ejecuta como procesos (`python -m scripts.ingesta`, `python -m scripts.publicar_gold`), sin abrir notebooks.
- [x] No hay secretos dentro del código ni en las evidencias (el script de evidencias los oculta y avisa si quedó alguno).
- [x] La ingesta escribe en Bronze y respeta la estrategia incremental.
- [ ] `dbt debug`, `run` y `test` finalizan correctamente → `run` y `test` en los logs de Airflow (`PASS=4`, `PASS=34`); falta `dbt debug` en `docs/evidencias/03_dbt.txt`.
- [x] Los modelos staging y marts tienen responsabilidades diferenciadas.
- [x] La DAG usa el `dag_id` y las rutas del grupo.
- [x] La ejecución completa termina correctamente en Airflow con las 5 tareas → `docs/capturas/airflow_dag_grid.png`, `airflow_dag_gantt.png` y logs.
- [x] El README permite reproducir el pipeline desde cero (ver también `README.md`, sección de puesta en marcha).
