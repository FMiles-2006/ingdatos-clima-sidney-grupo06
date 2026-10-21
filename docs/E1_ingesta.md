# Entrega 1 · Ingesta idempotente hacia bronze

Código: [`scripts/ingesta.py`](../scripts/ingesta.py) · Notebook: [`notebooks/NB02_semana04_extraccion_bronze.ipynb`](../notebooks/NB02_semana04_extraccion_bronze.ipynb) · Tests: [`ci/test_ingesta.py`](../ci/test_ingesta.py)

## Cómo correrlo (desde cero)

**En el servidor de la cátedra** (Jupyter de `grupo06`):
1. Ejecutar `notebooks/NB00_desplegar_en_servidor.ipynb`: clona el repo en `~/work/dags` y crea el `.env` con las credenciales que JupyterHub ya inyecta.
2. En una terminal de Jupyter:
   ```bash
   cd ~/work/dags
   python -m scripts.ingesta                 # 1ª vez: trae 2022-01-01 -> ayer
   python -m scripts.ingesta                 # 2ª vez: "Nada nuevo para pedir" (0 filas, 0 pedidos a la API)
   ```
   O bien correr `NB02` de punta a punta: hace las dos corridas y verifica con `assert` que no hay duplicados.

**En local** (Docker de la cátedra): `cp .env.example .env && make up && make work`, y dentro del contenedor los mismos comandos.

Los datos quedan en `s3://grupo06/bronze/clima_sidney` (Delta Lake). Para mirarlos: NB03, o la consola de MinIO.

## Watermark: qué campo y por qué

El watermark es **`max(fecha)` de la tabla bronze**: el último día de Sídney que ya tenemos.

- Es el campo natural: Open-Meteo entrega **un registro por día** y se consulta por rango de fechas (`start_date`, `end_date`).
- **Se deriva de los datos**, no de un archivo aparte: no se puede desincronizar de lo que realmente hay en bronze.
- **Se usa como parámetro de la consulta**: `start_date = watermark + 1 día`. El origen solo devuelve lo nuevo; no filtramos del lado del cliente.
- **Primera corrida** (tabla inexistente): el watermark es `None` y se pide desde `2022-01-01`. Solo "la tabla no existe" se interpreta así; cualquier otro error al leer bronze (credenciales, red) se propaga, para no recargar todo el histórico por un error.
- El límite superior **no sale del reloj**: llega como parámetro (`--fin`, en Airflow `data_interval_end`). `hasta = fin − 1 día`, así nunca pedimos el día en curso (que todavía es pronóstico).

### Dos APIs según la fecha
| Rango | API | Por qué |
|---|---|---|
| 2022-01-01 → 2025-12-31 | Archive API (reanálisis ERA5) | Mejor calidad para el histórico |
| 2026-01-01 → ayer | Historical Forecast API | ERA5 se publica con días de demora; esta tiene el día anterior completo |

La función `_tramos` parte cualquier rango pedido por fuente y por año calendario.

## Idempotencia

- **Corrida normal**: si `watermark + 1 > hasta`, **no se consulta la API**. No estamos deduplicando: no traemos lo que ya tenemos.
- **Escritura**: si todas las fechas son nuevas → `append`. Si el rango incluye fechas ya cargadas (reproceso o backfill, ver E3) → **`MERGE` por `fecha`** (upsert): actualiza, no agrega. Nunca `overwrite`.
- **Atomicidad**: Delta escribe una versión nueva de la tabla en un commit; si la corrida falla a mitad de camino, la versión anterior queda intacta.

Demo: `NB02`, sección 4 (dos corridas seguidas; la segunda deja igual las filas, las fechas únicas y la versión de Delta).

## Manejo de errores

| Situación | Qué hace el código | Tipo |
|---|---|---|
| La API no responde / tarda (timeout 5 s de conexión, 30 s de lectura) | Reintenta hasta 3 veces, con espera de 2 s y 4 s | Transitoria |
| HTTP 429 (rate limit) o 5xx | Igual: reintenta con backoff | Transitoria |
| Sigue fallando después de 3 intentos | `ErrorTransitorio`, sale con código 1; en Airflow, la tarea se reintenta más tarde (5 y 10 min) | Transitoria |
| HTTP 400/401/403/404 (parámetro inválido, URL mal) | `ErrorPermanente` inmediato, **sin reintentar**: repetir da lo mismo | Permanente |
| 200 pero el cuerpo no es JSON | `ErrorPermanente` | Datos raros |
| JSON sin bloque `daily`, faltan variables, columnas de distinto largo | `ErrorPermanente` (se verifica **antes** de convertir a tabla) | Datos raros |
| Fechas inválidas, repetidas o fuera del rango pedido | `ErrorPermanente` | Datos raros |
| Unidad de temperatura distinta de °C | `ErrorPermanente` (un cambio de unidad arruinaría la serie sin dar error) | Datos raros |
| Días con **todas** las mediciones nulas (el origen todavía no los publicó) | No se escriben y el corte se hace en el primer hueco: el watermark no avanza y la próxima corrida los vuelve a pedir | Esperable |

Con cualquier error, **no se escribe nada**: se valida todo antes de escribir y la escritura Delta es atómica.

Todo esto está cubierto por tests automáticos sin red (`ci/test_ingesta.py`), que corren en cada push.
