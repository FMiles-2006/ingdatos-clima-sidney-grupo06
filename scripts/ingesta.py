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


# ---------------------------------------------------------------------------
# 1. Watermark
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# 2. Extracción (con manejo de errores)
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


def _validar(js: dict, desde: date, hasta: date, fuente: str) -> pd.DataFrame:
    """Convierte la respuesta en DataFrame verificando que tenga la forma esperada."""
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
    unidad = js.get("daily_units", {}).get("temperature_2m_max")
    if unidad != "°C":
        raise ErrorPermanente(f"Unidad de temperatura inesperada: {unidad!r}")

    df = pd.DataFrame(diario).rename(columns={"time": "fecha"})
    fechas = pd.to_datetime(df["fecha"], format="%Y-%m-%d", errors="coerce")
    if fechas.isna().any() or fechas.duplicated().any():
        raise ErrorPermanente("Fechas inválidas o repetidas en la respuesta")
    if fechas.min().date() < desde or fechas.max().date() > hasta:
        raise ErrorPermanente(f"La API devolvió fechas fuera de [{desde}, {hasta}]")

    # Un día con TODAS las mediciones nulas todavía no está publicado en el origen.
    # No se escribe: así el watermark no avanza y la próxima corrida lo vuelve a pedir.
    medidas = [v for v in VARIABLES if v != "weather_code"]
    vacio = df[medidas].isna().all(axis=1)
    if vacio.any():
        log.warning("%d día(s) sin datos todavía en %s (desde %s): se pedirán en la próxima corrida",
                    int(vacio.sum()), fuente, df.loc[vacio, "fecha"].min())
        primero_vacio = df.loc[vacio, "fecha"].min()
        df = df[df["fecha"] < primero_vacio]      # cortar en el primer hueco: sin saltos

    df[VARIABLES] = df[VARIABLES].astype("float64")
    df["fuente"] = fuente
    df["latitud"], df["longitud"] = LATITUD, LONGITUD
    return df


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


def extraer(desde: date, hasta: date) -> pd.DataFrame:
    """Pide a Open-Meteo SOLO el rango [desde, hasta]."""
    partes = []
    for url, fuente, d, h in _tramos(desde, hasta):
        log.info("Pidiendo %s -> %s a %s", d, h, fuente)
        js = _pedir(url, {
            "latitude": LATITUD, "longitude": LONGITUD, "timezone": ZONA_HORARIA,
            "start_date": d.isoformat(), "end_date": h.isoformat(),
            "daily": ",".join(VARIABLES),
        })
        parte = _validar(js, d, h, fuente)
        partes.append(parte)
        if len(parte) < (h - d).days + 1:      # el origen todavía no tiene el final del tramo
            break
    if not partes:
        return pd.DataFrame()
    df = pd.concat(partes, ignore_index=True)
    df["ingestado_en"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    return df


# ---------------------------------------------------------------------------
# 3. Escritura en bronze
# ---------------------------------------------------------------------------
def escribir_bronze(df: pd.DataFrame, watermark: date | None) -> str:
    from deltalake import DeltaTable, write_deltalake

    tabla = pa.Table.from_pandas(df, preserve_index=False)
    reproceso = watermark is not None and df["fecha"].min() <= watermark.isoformat()
    if not reproceso:
        write_deltalake(BRONZE, tabla, mode="append", storage_options=STORAGE_OPTIONS)
        return "append"
    (DeltaTable(BRONZE, storage_options=STORAGE_OPTIONS)
        .merge(source=tabla, predicate="t.fecha = s.fecha", source_alias="s", target_alias="t")
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute())
    return "merge"


def ingerir(inicio: date | None = None, fin: date | None = None) -> int:
    """Corre una ingesta para el intervalo [inicio, fin). Devuelve filas escritas."""
    fin = fin or date.today()                       # solo en corridas manuales; Airflow siempre pasa --fin
    hasta = fin - timedelta(days=1)
    wm = leer_watermark()
    desde = FECHA_INICIO if wm is None else wm + timedelta(days=1)
    if inicio is not None:
        desde = max(FECHA_INICIO, min(desde, inicio))
    log.info("Watermark: %s | intervalo pedido: [%s, %s) | a extraer: %s -> %s",
             wm, inicio, fin, desde, hasta)

    if desde > hasta:
        log.info("Nada nuevo para pedir: bronze ya está al día. No se consulta la API.")
        return 0

    nuevos = extraer(desde, hasta)
    if nuevos.empty:
        log.info("El origen todavía no publicó datos para ese rango.")
        return 0
    modo = escribir_bronze(nuevos, wm)
    log.info("Escritas %d filas en %s (%s), fechas %s -> %s",
             len(nuevos), BRONZE, modo, nuevos["fecha"].min(), nuevos["fecha"].max())
    return len(nuevos)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Ingesta Open-Meteo -> bronze")
    p.add_argument("--inicio", type=date.fromisoformat, help="data_interval_start (YYYY-MM-DD)")
    p.add_argument("--fin", type=date.fromisoformat, help="data_interval_end (YYYY-MM-DD, exclusivo)")
    a = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        ingerir(a.inicio, a.fin)
    except ErrorPermanente as e:
        log.error("FALLA PERMANENTE (reintentar no sirve, hay que revisar): %s", e)
        return 2
    except ErrorTransitorio as e:
        log.error("FALLA TRANSITORIA (Airflow reintentará la tarea): %s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
