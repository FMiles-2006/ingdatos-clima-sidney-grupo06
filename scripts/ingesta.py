"""
Ingesta incremental e idempotente: Open-Meteo -> bronze (Delta Lake sobre MinIO).

Dominio: clima diario de Sídney (Australia), desde 2022-01-01.

DOS FUENTES (dos APIs de Open-Meteo), elegidas según la fecha:
  * Histórico  (fecha <= 2025-12-31): Archive API, reanálisis ERA5.
        https://archive-api.open-meteo.com/v1/archive
  * Incremental (fecha >= 2026-01-01): Historical Forecast API.
        https://historical-forecast-api.open-meteo.com/v1/forecast
    El reanálisis ERA5 se publica con días de demora; la Historical Forecast API
    tiene el día anterior completo, por eso es la que alimenta la carga diaria.

WATERMARK: la máxima `fecha` ya presente en bronze. Se deriva de los datos (no de
un archivo aparte) y se usa como PARÁMETRO de la consulta (start_date = watermark+1):
el origen solo entrega lo nuevo.

INTERVALO: el rango a procesar llega de afuera (`--inicio` / `--fin`). En Airflow son
`data_interval_start` y `data_interval_end` de la corrida, nunca el reloj del sistema;
eso es lo que hace posible el backfill.

    desde = min(watermark + 1, inicio)      # reproceso si el intervalo ya estaba cargado
    hasta = fin - 1 día                     # el intervalo es [inicio, fin)

ESCRITURA:
  * solo fechas nuevas            -> append (lo normal todos los días)
  * hay fechas ya cargadas        -> MERGE por fecha (upsert: backfill/reproceso)
  En ambos casos, reejecutar no duplica.

ESTRUCTURA (una función por responsabilidad, en el orden en que corren):
    cargar_configuracion -> extraer_datos -> transformar_datos -> guardar_bronze
    ingerir_incremental() las encadena; es lo que ejecuta Airflow.

CÓDIGOS DE SALIDA: 0 ok · 1 falla transitoria (reintentable) · 2 falla permanente ·
3 configuración inválida (falta una variable del .env).

Uso:
    python -m scripts.ingesta                                   # manual: completa hasta ayer
    python -m scripts.ingesta --inicio 2026-03-01 --fin 2026-03-08   # reprocesa una semana
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
import requests

# entorno.py vive en la raíz del proyecto (un nivel arriba de scripts/)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entorno import BRONZE, STORAGE_OPTIONS  # noqa: E402

log = logging.getLogger("ingesta")

# ---------------------------------------------------------------------------
# Parámetros del dominio
# ---------------------------------------------------------------------------
LATITUD, LONGITUD = -33.7786, 151.2314          # Sídney (Lane Cove / North Shore)
ZONA_HORARIA = "Australia/Sydney"               # el "día" es el día local de Sídney
FECHA_INICIO = date(2022, 1, 1)                 # primera fecha del dataset
FECHA_CORTE = date(2025, 12, 31)                # último día servido por la Archive API

URL_ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
URL_HIST_FORECAST = "https://historical-forecast-api.open-meteo.com/v1/forecast"

VARIABLES = [
    "weather_code",                 # código WMO del estado del tiempo
    "temperature_2m_max",           # °C
    "temperature_2m_min",           # °C
    "temperature_2m_mean",          # °C
    "apparent_temperature_mean",    # °C (sensación térmica)
    "precipitation_sum",            # mm
    "rain_sum",                     # mm
    "precipitation_hours",          # h
    "wind_speed_10m_max",           # km/h
    "wind_gusts_10m_max",           # km/h
    "wind_direction_10m_dominant",  # grados
    "shortwave_radiation_sum",      # MJ/m²
]

REINTENTOS = 3                  # intentos por pedido ante fallas transitorias
ESPERA_BASE_S = 2               # backoff exponencial: 2 s, 4 s, 8 s
TIMEOUT_S = (5, 30)             # (conexión, lectura)


class ErrorTransitorio(RuntimeError):
    """La API no respondió / respondió 429 o 5xx. Reintentar puede resolverlo."""


class ErrorPermanente(RuntimeError):
    """Pedido inválido (4xx) o respuesta con forma inesperada. Reintentar NO lo resuelve."""


class ErrorConfiguracion(RuntimeError):
    """Falta una variable obligatoria o tiene un valor inválido. Hay que corregir el .env."""


# ---------------------------------------------------------------------------
# 1. Configuración y watermark
# ---------------------------------------------------------------------------
# Variables obligatorias cuando bronze vive en MinIO/S3. Se leen del entorno (que
# entorno.py completa con el .env del grupo); NUNCA se escriben en el código.
VARIABLES_S3 = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_ENDPOINT_URL"]


def leer_watermark() -> date | None:
    """Máxima fecha ya cargada en bronze, o None si la tabla todavía no existe.

    Solo "la tabla no existe" devuelve None. Cualquier otro error (credenciales, red)
    se propaga: si devolviéramos None ante cualquier falla, la corrida recargaría
    todo el histórico.
    """
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(BRONZE, storage_options=STORAGE_OPTIONS):
        return None
    fechas = DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS).to_pyarrow_table(columns=["fecha"])
    if fechas.num_rows == 0:
        return None
    return date.fromisoformat(max(fechas.column("fecha").to_pylist()))


def cargar_configuracion(inicio: date | None = None, fin: date | None = None) -> dict:
    """Lee y valida la configuración, y decide QUÉ rango pedir a la API.

    * Verifica las variables obligatorias (sin mostrar sus valores): si falta una,
      falla con un mensaje claro en vez de usar un valor por defecto en silencio.
    * Lee el watermark y calcula el rango incremental:
          desde = min(watermark + 1, inicio)   # reproceso si el intervalo ya estaba cargado
          hasta = fin - 1 día                  # el intervalo de Airflow es [inicio, fin)
    """
    if not BRONZE:
        raise ErrorConfiguracion("No está definido el destino bronze (BRONZE_PATH o MINIO_BUCKET).")
    if BRONZE.startswith("s3://"):
        faltan = [v for v in VARIABLES_S3 if not os.environ.get(v)]
        if faltan:
            raise ErrorConfiguracion(
                f"Faltan variables obligatorias en el .env o el entorno: {', '.join(faltan)}. "
                "En el servidor se generan con notebooks/NB00 (ver README_GRUPO.md).")

    fin = fin or date.today()                       # solo en corridas manuales; Airflow siempre pasa --fin
    if inicio is not None and inicio >= fin:
        raise ErrorConfiguracion(f"Intervalo vacío o invertido: inicio={inicio} fin={fin}")
    hasta = fin - timedelta(days=1)
    wm = leer_watermark()
    desde = FECHA_INICIO if wm is None else wm + timedelta(days=1)
    if inicio is not None:
        desde = max(FECHA_INICIO, min(desde, inicio))
    config = {"bronze": BRONZE, "watermark": wm, "inicio": inicio, "fin": fin,
              "desde": desde, "hasta": hasta}
    log.info("Destino bronze: %s | watermark: %s | intervalo pedido: [%s, %s) | a extraer: %s -> %s",
             BRONZE, wm, inicio, fin, desde, hasta)
    return config


# ---------------------------------------------------------------------------
# 2. Extracción (llamada a la API + validación de la respuesta)
# ---------------------------------------------------------------------------
def _pedir(url: str, params: dict) -> dict:
    """GET con reintentos SOLO para fallas transitorias. Verifica el estado antes de parsear."""
    for intento in range(1, REINTENTOS + 1):
        try:
            r = requests.get(url, params=params, timeout=TIMEOUT_S)
        except (requests.Timeout, requests.ConnectionError) as e:
            causa = f"sin respuesta ({type(e).__name__})"
        else:
            if r.status_code == 200:
                try:
                    return r.json()
                except ValueError as e:
                    raise ErrorPermanente(f"La API devolvió 200 pero el cuerpo no es JSON: {r.text[:200]!r}") from e
            if r.status_code == 429 or r.status_code >= 500:
                causa = f"HTTP {r.status_code}"
            else:  # 400, 401, 403, 404...: el pedido está mal; repetirlo da lo mismo
                raise ErrorPermanente(f"HTTP {r.status_code} en {url}: {r.text[:300]}")
        if intento < REINTENTOS:
            espera = ESPERA_BASE_S * 2 ** (intento - 1)
            log.warning("Falla transitoria (%s). Reintento %d/%d en %ds", causa, intento, REINTENTOS - 1, espera)
            time.sleep(espera)
    raise ErrorTransitorio(f"{url} sigue fallando tras {REINTENTOS} intentos: {causa}")


def _validar_respuesta(js: dict, desde: date, hasta: date) -> None:
    """Verifica que la respuesta tenga la forma esperada. Si no, ErrorPermanente."""
    if js.get("error"):
        raise ErrorPermanente(f"La API informó un error: {js.get('reason')}")
    diario = js.get("daily")
    if not isinstance(diario, dict) or "time" not in diario:
        raise ErrorPermanente(f"Respuesta sin bloque 'daily': claves {list(js)}")
    faltan = [v for v in VARIABLES if v not in diario]
    if faltan:
        raise ErrorPermanente(f"La API no devolvió las variables {faltan}")
    largos = {k: len(v) for k, v in diario.items()}
    if len(set(largos.values())) != 1:
        raise ErrorPermanente(f"Columnas de distinto largo: {largos}")
    if largos["time"] == 0:
        raise ErrorPermanente(f"La API devolvió una respuesta vacía para {desde} -> {hasta}")
    unidad = js.get("daily_units", {}).get("temperature_2m_max")
    if unidad != "°C":
        raise ErrorPermanente(f"Unidad de temperatura inesperada: {unidad!r}")
    fechas = pd.to_datetime(pd.Series(diario["time"]), format="%Y-%m-%d", errors="coerce")
    if fechas.isna().any() or fechas.duplicated().any():
        raise ErrorPermanente("Fechas inválidas o repetidas en la respuesta")
    if fechas.min().date() < desde or fechas.max().date() > hasta:
        raise ErrorPermanente(f"La API devolvió fechas fuera de [{desde}, {hasta}]")


def _dias_publicados(diario: dict) -> int:
    """Cuántos días seguidos, desde el primero, ya tienen alguna medición publicada."""
    medidas = [v for v in VARIABLES if v != "weather_code"]
    for i in range(len(diario["time"])):
        if all(diario[m][i] is None for m in medidas):
            return i
    return len(diario["time"])


def _tramos(desde: date, hasta: date):
    """Parte [desde, hasta] por fuente (corte 2025-12-31) y por año calendario."""
    d = desde
    while d <= hasta:
        if d <= FECHA_CORTE:
            url, fuente, tope = URL_ARCHIVE, "archive_era5", min(hasta, FECHA_CORTE)
        else:
            url, fuente, tope = URL_HIST_FORECAST, "historical_forecast", hasta
        h = min(tope, date(d.year, 12, 31))       # un pedido por año: respuestas chicas, reintentos baratos
        yield url, fuente, d, h
        d = h + timedelta(days=1)


def extraer_datos(config: dict) -> list[dict]:
    """Consulta la API SOLO para el rango [desde, hasta] y devuelve las respuestas validadas.

    Cada elemento es {"fuente", "desde", "hasta", "daily"}: el bloque diario crudo tal
    como lo entrega Open-Meteo. No transforma nada (eso es transformar_datos).
    """
    desde, hasta = config["desde"], config["hasta"]
    registros = []
    if desde > hasta:
        return registros
    for url, fuente, d, h in _tramos(desde, hasta):
        log.info("Pidiendo %s -> %s a %s", d, h, fuente)
        js = _pedir(url, {
            "latitude": LATITUD, "longitude": LONGITUD, "timezone": ZONA_HORARIA,
            "start_date": d.isoformat(), "end_date": h.isoformat(),
            "daily": ",".join(VARIABLES),
        })
        _validar_respuesta(js, d, h)
        registros.append({"fuente": fuente, "desde": d, "hasta": h, "daily": js["daily"]})
        if _dias_publicados(js["daily"]) < (h - d).days + 1:   # el origen todavía no tiene el final
            break
    log.info("Obtenidos %d día(s) de la API en %d pedido(s)",
             sum(len(r["daily"]["time"]) for r in registros), len(registros))
    return registros


# ---------------------------------------------------------------------------
# 3. Transformación mínima (bronze = crudo, pero tipado y con trazabilidad)
# ---------------------------------------------------------------------------
def transformar_datos(registros: list[dict]) -> pd.DataFrame:
    """Normaliza nombres y tipos y agrega columnas de control.

    * `time` -> `fecha` (texto YYYY-MM-DD, la clave temporal / watermark).
    * Variables numéricas a float64 (los nulos de la API quedan como NaN).
    * Un día con TODAS las mediciones nulas todavía no está publicado en el origen: se
      descarta (y lo que viene después), así el watermark no avanza y la próxima corrida
      lo vuelve a pedir. Sin huecos en la serie.
    * Agrega fuente, coordenadas e `ingestado_en` (auditoría).
    La limpieza de negocio (renombres en español, rain_sum, dirección del viento) NO va
    acá: es responsabilidad de dbt (staging).
    """
    partes = []
    for reg in registros:
        df = pd.DataFrame(reg["daily"]).rename(columns={"time": "fecha"})
        publicados = _dias_publicados(reg["daily"])
        if publicados < len(df):
            log.warning("%d día(s) sin datos todavía en %s (desde %s): se pedirán en la próxima corrida",
                        len(df) - publicados, reg["fuente"], df["fecha"].iloc[publicados])
            df = df.iloc[:publicados]
        if df.empty:
            continue
        df[VARIABLES] = df[VARIABLES].astype("float64")
        df["fuente"] = reg["fuente"]
        df["latitud"], df["longitud"] = LATITUD, LONGITUD
        partes.append(df)
    if not partes:
        return pd.DataFrame()
    datos = pd.concat(partes, ignore_index=True)
    datos["ingestado_en"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return datos


def extraer(desde: date, hasta: date) -> pd.DataFrame:
    """Atajo usado por NB02: extraer + transformar un rango sin escribir nada."""
    return transformar_datos(extraer_datos({"desde": desde, "hasta": hasta}))


# ---------------------------------------------------------------------------
# 4. Escritura en bronze
# ---------------------------------------------------------------------------
def guardar_bronze(datos: pd.DataFrame, config: dict) -> dict:
    """Escribe en bronze sin duplicar.

    * solo fechas posteriores al watermark -> append (lo normal todos los días)
    * hay fechas ya cargadas (backfill)    -> MERGE por fecha (upsert)
    """
    from deltalake import DeltaTable, write_deltalake

    if datos.empty:
        return {"modo": "sin_cambios", "escritas": 0}
    wm = config["watermark"]
    tabla = pa.Table.from_pandas(datos, preserve_index=False)
    reproceso = wm is not None and datos["fecha"].min() <= wm.isoformat()
    if not reproceso:
        write_deltalake(BRONZE, tabla, mode="append", storage_options=STORAGE_OPTIONS)
        modo = "append"
    else:
        (DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS)
            .merge(source=tabla, predicate="t.fecha = s.fecha", source_alias="s", target_alias="t")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute())
        modo = "merge"
    return {"modo": modo, "escritas": len(datos),
            "desde": datos["fecha"].min(), "hasta": datos["fecha"].max()}


# ---------------------------------------------------------------------------
# 5. Orquestación del script (lo que llama Airflow)
# ---------------------------------------------------------------------------
def ingerir_incremental(inicio: date | None = None, fin: date | None = None) -> dict:
    """Corre una ingesta para el intervalo [inicio, fin) y devuelve un resumen."""
    config = cargar_configuracion(inicio, fin)
    if config["desde"] > config["hasta"]:
        log.info("Nada nuevo para pedir: bronze ya está al día. No se consulta la API.")
        resultado = {"modo": "sin_cambios", "obtenidas": 0, "escritas": 0}
    else:
        registros = extraer_datos(config)
        datos = transformar_datos(registros)
        resultado = guardar_bronze(datos, config)
        resultado["obtenidas"] = sum(len(r["daily"]["time"]) for r in registros)
        if datos.empty:
            log.info("El origen todavía no publicó datos para ese rango.")
    if resultado["escritas"]:
        log.info("Escritas %d filas en %s (%s), fechas %s -> %s",
                 resultado["escritas"], BRONZE, resultado["modo"], resultado["desde"], resultado["hasta"])
    log.info("RESUMEN: obtenidas de la API=%d | escritas en bronze=%d | modo=%s",
             resultado["obtenidas"], resultado["escritas"], resultado["modo"])
    return resultado


def ingerir(inicio: date | None = None, fin: date | None = None) -> int:
    """Igual que ingerir_incremental pero devuelve solo las filas escritas (NB02 y tests)."""
    return ingerir_incremental(inicio, fin)["escritas"]


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Ingesta Open-Meteo -> bronze")
    p.add_argument("--inicio", type=date.fromisoformat, help="data_interval_start (YYYY-MM-DD)")
    p.add_argument("--fin", type=date.fromisoformat, help="data_interval_end (YYYY-MM-DD, exclusivo)")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        ingerir_incremental(a.inicio, a.fin)
    except ErrorConfiguracion as e:
        log.error("CONFIGURACIÓN INVÁLIDA: %s", e)
        return 3
    except ErrorPermanente as e:
        log.error("FALLA PERMANENTE (reintentar no sirve, hay que revisar): %s", e)
        return 2
    except ErrorTransitorio as e:
        log.error("FALLA TRANSITORIA (Airflow reintentará la tarea): %s", e)
        return 1
    return 0


if __name__ == "__main__":
    codigo = main()
    # Workaround: con deltalake 1.x + Python 3.13 el proceso puede quedar colgado al
    # finalizar el intérprete (hilos nativos del runtime de I/O de deltalake), aunque
    # todo el trabajo ya terminó. Vaciamos los buffers y salimos explícitamente para
    # que Airflow vea el código de salida real en vez de un timeout.
    logging.shutdown()
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(codigo)
