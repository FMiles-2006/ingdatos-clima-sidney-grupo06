# Clima de Sídney — pipeline de ingeniería de datos (grupo06)

Proyecto integrador de **Ingeniería de Datos** · Licenciatura en Ciencia de Datos · UCA Rosario · 2026.

Pipeline completo y automático que todos los días trae el clima diario de Sídney (Australia) desde
**Open-Meteo**, lo guarda crudo en un **lake** (Delta Lake sobre MinIO), lo limpia y agrega con
**dbt + DuckDB**, lo publica en **PostgreSQL** y lo muestra en un tablero de **Metabase**,
orquestado por **Airflow** y verificado por **CI** en cada cambio.

```
 Open-Meteo                    MinIO (s3://grupo06)        dbt + DuckDB                     PostgreSQL           Metabase
 ┌───────────────┐  ingesta   ┌──────────────────┐  dbt   ┌───────────────────────────┐ pub ┌───────────────┐   ┌──────────┐
 │ Archive ERA5  │──────────► │ bronze           │──────► │ silver  stg_clima_diario  │───► │ grupo06.      │──►│ tablero  │
 │ (≤ 2025-12-31)│ watermark  │ clima_sidney     │ build  │         stg_codigos_clima │     │ fct_clima_    │   │ 4 gráf.  │
 │ Hist.Forecast │ + MERGE    │ (Delta, crudo,   │ + test │ gold    fct_clima_mensual │     │  mensual      │   └──────────┘
 │ (≥ 2026-01-01)│            │  versionado)     │        │         feat_clima_diario │     │ feat_clima_   │
 └───────────────┘            └──────────────────┘        └───────────────────────────┘     │  diario       │
                                                                                             └───────────────┘
        └──────────────── Airflow: preparar_entorno ► extraer_bronze ► dbt_run ► dbt_test ► publicar_gold (06:00 ART) ┘
```

## Dominio y fuentes

- **Qué:** variables diarias del clima en Sídney (−33.78, 151.23), día local `Australia/Sydney`, desde el **2022-01-01**.
- **Dos APIs de Open-Meteo** (sin clave, licencia CC BY 4.0):
  | Rango | API | Motivo |
  |---|---|---|
  | 2022-01-01 → 2025-12-31 | [Archive API](https://open-meteo.com/en/docs/historical-weather-api) (reanálisis ERA5) | histórico de mejor calidad |
  | 2026-01-01 → ayer | [Historical Forecast API](https://open-meteo.com/en/docs/historical-forecast-api) | tiene el día anterior completo; ERA5 llega con días de demora |
- **No se usa ningún CSV**: todo sale de la API y el **watermark** completa lo que falta en cada corrida.

## Estructura

```
.
├── dags/pipeline_grupo06.py      # DAG (E3)
├── scripts/
│   ├── ingesta.py                # API -> bronze: watermark, errores, append/MERGE (E1)
│   ├── publicar_gold.py          # gold DuckDB -> PostgreSQL, idempotente (E4)
│   └── preparar_entorno.sh       # venv propio del grupo dentro del Airflow compartido
├── dbt/                          # models/staging (silver) + models/marts (gold) + tests + docs (E2/E4)
├── ci/                           # tests de CI + bronze de muestra
├── .github/workflows/ci.yml      # integración continua (E3)
├── notebooks/NB00..NB06          # notebooks de las prácticas, adaptados a Sídney
├── docs/                         # README de cada entrega, governance, tablero, defensa
├── entorno.py                    # configuración única (servidor de la cátedra o Docker local)
├── .airflowignore                # Airflow solo parsea dags/
└── .env.example                  # plantilla de credenciales (.env NO se versiona)
```

## Documentación por entrega

| Entrega | Documento |
|---|---|
| E1 · Ingesta idempotente → bronze | [docs/E1_ingesta.md](docs/E1_ingesta.md) |
| E2 · dbt staging (silver) / marts (gold) + calidad | [docs/E2_dbt.md](docs/E2_dbt.md) |
| E3 · Airflow + CI | [docs/E3_airflow_ci.md](docs/E3_airflow_ci.md) |
| E4 · Serving, features, tablero | [docs/E4_serving.md](docs/E4_serving.md) · [docs/tablero_metabase.md](docs/tablero_metabase.md) |
| Governance del dataset | [docs/governance.md](docs/governance.md) |
| Defensa | [docs/defensa.md](docs/defensa.md) |
| Actividad integradora (scripts + dbt + Airflow) | [README_GRUPO.md](README_GRUPO.md) · evidencias en [docs/evidencias/](docs/evidencias/) |

## Puesta en marcha en el servidor de la cátedra (lo que se usa en la defensa)

El Airflow compartido **solo monta `~/work/dags`** de cada grupo. Por eso el repo se clona **dentro** de esa carpeta.

1. Abrir https://jupyter.nexoacademia.org (usuario `grupo06`).
2. Subir `notebooks/NB00_desplegar_en_servidor.ipynb` (o copiar su primera celda) y ejecutarlo:
   clona/actualiza el repo en `~/work/dags` y genera el `.env` desde las variables de JupyterHub.
3. Abrir `~/work/dags/notebooks/` y correr NB02 → NB06 en orden (primera carga, dbt, publicación).
4. En https://airflow.nexoacademia.org activar el DAG **`pipeline_grupo06`**.
5. En https://metabase.nexoacademia.org rehacer el tablero según [docs/tablero_metabase.md](docs/tablero_metabase.md).

Actualizar después de un cambio en GitHub: volver a ejecutar NB00 (hace `git pull`).

## Puesta en marcha local (Docker de la cátedra)

```bash
cp .env.example .env
make up                                  # minio, postgres, airflow, metabase
make work                                # shell en el contenedor de trabajo
python -m scripts.ingesta                # 1ª corrida: histórico 2022 -> ayer
cd dbt && dbt build --profiles-dir .     # silver + gold + tests
cd .. && python -m scripts.publicar_gold # gold -> PostgreSQL
```

## Estado (verificado en el servidor de la cátedra el 21/10/2026)

| Parte | Estado |
|---|---|
| Ingesta (E1) | ✅ 1.754 días en bronze (2022-01-01 → 2026-10-20); 2ª corrida sin pedidos a la API |
| dbt (E2) | ✅ `dbt build`: 1 seed, 4 modelos, 34 tests en verde sobre la tabla Delta real |
| Airflow (E3) | ✅ `pipeline_grupo06` en verde de punta a punta; backfill por rango y por fecha lógica probado |
| CI (E3) | ✅ 3 jobs en verde; `main` protegida; PR #1 de demostración en rojo y bloqueado |
| Gold en PostgreSQL (E4) | ✅ `grupo06.fct_clima_mensual` (58 meses) y `grupo06.feat_clima_diario`; publicación repetible |
| Notebooks NB01-NB06 | ✅ ejecutados de punta a punta en el servidor |
| Tablero Metabase (E4) | ⏳ **la base `warehouse` no está conectada en Metabase para grupo06**; agregar la conexión requiere un administrador (pedírselo a la cátedra). Las preguntas están definidas en [docs/tablero_metabase.md](docs/tablero_metabase.md) |
| Governance | ✅ 6 secciones; falta confirmar el **responsable** (nombre y apellido) |

## Equipo

grupo06: Bautista Verna, Francisco Miles, Tomás Lizazu, Martín Masuelli, Juan Ancarani y Felipe Dal Lago.
El responsable del dataset figura en la sección *Responsable* de [docs/governance.md](docs/governance.md).
