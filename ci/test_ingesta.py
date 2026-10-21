"""
Tests de la ingesta (corren en CI, sin red y sin MinIO).

La API de Open-Meteo se reemplaza por un doble que responde según las fechas pedidas,
y bronze es una tabla Delta en una carpeta temporal. Así verificamos el comportamiento
que pide E1 (incremental, idempotente, manejo de errores) en cada push.
"""
from __future__ import annotations

import importlib
import os
import sys
from datetime import date, timedelta

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)


class RespuestaFalsa:
    def __init__(self, status=200, js=None, texto=""):
        self.status_code, self._js, self.text = status, js, texto

    def json(self):
        if self._js is None:
            raise ValueError("no es JSON")
        return self._js


def _respuesta_ok(params, hasta_publicado=None):
    """Respuesta válida de Open-Meteo para el rango pedido. Los días posteriores a
    `hasta_publicado` vienen con nulos (todavía no publicados en el origen)."""
    import scripts.ingesta as ing
    d0, d1 = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
    dias = [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]
    diario = {"time": [d.isoformat() for d in dias]}
    for v in ing.VARIABLES:
        diario[v] = [None if (hasta_publicado and d > hasta_publicado) else 20.0 for d in dias]
    return {"daily": diario, "daily_units": {"temperature_2m_max": "°C"}}


@pytest.fixture
def ing(tmp_path, monkeypatch):
    monkeypatch.setenv("BRONZE_PATH", str(tmp_path / "bronze"))
    import entorno
    importlib.reload(entorno)
    import scripts.ingesta as modulo
    importlib.reload(modulo)
    monkeypatch.setattr(modulo.time, "sleep", lambda s: None)   # sin esperas reales
    modulo.PEDIDOS = []

    def get_falso(url, params, timeout):
        modulo.PEDIDOS.append((url, params["start_date"], params["end_date"]))
        return RespuestaFalsa(200, _respuesta_ok(params))

    monkeypatch.setattr(modulo.requests, "get", get_falso)
    return modulo


def _filas(ing):
    from deltalake import DeltaTable
    df = DeltaTable(ing.BRONZE).to_pandas()
    return len(df), df["fecha"].nunique()


def test_primera_corrida_trae_historico_con_las_dos_apis(ing):
    n = ing.ingerir(fin=date(2026, 1, 11))
    assert n == (date(2026, 1, 10) - ing.FECHA_INICIO).days + 1
    urls = {u for u, _, _ in ing.PEDIDOS}
    assert urls == {ing.URL_ARCHIVE, ing.URL_HIST_FORECAST}
    # La Archive API nunca recibe fechas posteriores al corte
    assert all(fin <= "2025-12-31" for u, _, fin in ing.PEDIDOS if u == ing.URL_ARCHIVE)


def test_segunda_corrida_no_consulta_la_api_ni_duplica(ing):
    ing.ingerir(fin=date(2026, 1, 11))
    antes = _filas(ing)
    ing.PEDIDOS.clear()
    assert ing.ingerir(fin=date(2026, 1, 11)) == 0
    assert ing.PEDIDOS == []                       # no trajo lo que ya tenía
    assert _filas(ing) == antes


def test_incremental_pide_desde_el_watermark(ing):
    ing.ingerir(fin=date(2026, 1, 11))
    ing.PEDIDOS.clear()
    ing.ingerir(inicio=date(2026, 1, 11), fin=date(2026, 1, 13))
    assert ing.PEDIDOS == [(ing.URL_HIST_FORECAST, "2026-01-11", "2026-01-12")]


def test_backfill_de_un_periodo_cargado_reprocesa_sin_duplicar(ing):
    ing.ingerir(fin=date(2026, 1, 11))
    antes = _filas(ing)
    ing.PEDIDOS.clear()
    n = ing.ingerir(inicio=date(2024, 3, 1), fin=date(2024, 3, 8))
    assert n == 7
    assert ing.PEDIDOS[0][1] == "2024-03-01"
    assert _filas(ing) == antes                    # MERGE: actualiza, no agrega


def test_dias_no_publicados_no_avanzan_el_watermark(ing, monkeypatch):
    def get_parcial(url, params, timeout):
        return RespuestaFalsa(200, _respuesta_ok(params, hasta_publicado=date(2022, 1, 5)))
    monkeypatch.setattr(ing.requests, "get", get_parcial)
    ing.ingerir(fin=date(2022, 1, 11))
    assert ing.leer_watermark() == date(2022, 1, 5)


def test_error_transitorio_reintenta_y_despues_falla(ing, monkeypatch):
    llamadas = []

    def get_caida(url, params, timeout):
        llamadas.append(1)
        return RespuestaFalsa(503, texto="Service Unavailable")
    monkeypatch.setattr(ing.requests, "get", get_caida)
    with pytest.raises(ing.ErrorTransitorio):
        ing.ingerir(fin=date(2022, 1, 3))
    assert len(llamadas) == ing.REINTENTOS


def test_error_transitorio_se_recupera(ing, monkeypatch):
    respuestas = iter([RespuestaFalsa(429), None])

    def get_flaky(url, params, timeout):
        r = next(respuestas)
        return r if r is not None else RespuestaFalsa(200, _respuesta_ok(params))
    monkeypatch.setattr(ing.requests, "get", get_flaky)
    assert ing.ingerir(fin=date(2022, 1, 3)) == 2


def test_error_permanente_no_reintenta(ing, monkeypatch):
    llamadas = []

    def get_400(url, params, timeout):
        llamadas.append(1)
        return RespuestaFalsa(400, {"error": True, "reason": "Parameter 'daily' is invalid"}, "bad request")
    monkeypatch.setattr(ing.requests, "get", get_400)
    with pytest.raises(ing.ErrorPermanente):
        ing.ingerir(fin=date(2022, 1, 3))
    assert len(llamadas) == 1


@pytest.mark.parametrize("cuerpo", [
    {"hourly": {}},                                                     # sin bloque daily
    {"daily": {"time": ["2022-01-01"], "temperature_2m_max": [1, 2]}},  # largos distintos / faltan variables
])
def test_respuesta_con_forma_inesperada_es_permanente(ing, monkeypatch, cuerpo):
    monkeypatch.setattr(ing.requests, "get", lambda url, params, timeout: RespuestaFalsa(200, cuerpo))
    with pytest.raises(ing.ErrorPermanente):
        ing.ingerir(fin=date(2022, 1, 2))
    assert ing.leer_watermark() is None          # no se escribió nada a medias


# ---- Controles mínimos de la actividad integradora ----

def test_falta_variable_obligatoria_falla_con_mensaje_claro(ing, monkeypatch):
    monkeypatch.setattr(ing, "BRONZE", "s3://grupo06/bronze/clima_sidney")
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)
    with pytest.raises(ing.ErrorConfiguracion, match="AWS_SECRET_ACCESS_KEY"):
        ing.cargar_configuracion(fin=date(2022, 1, 3))
    assert ing.PEDIDOS == []                       # ni siquiera llega a consultar la API


def test_main_devuelve_codigo_3_si_falta_configuracion(ing, monkeypatch):
    monkeypatch.setattr(ing, "BRONZE", "s3://grupo06/bronze/clima_sidney")
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    assert ing.main(["--fin", "2022-01-03"]) == 3


def test_respuesta_vacia_es_permanente(ing, monkeypatch):
    vacia = {"daily": {"time": [], **{v: [] for v in ing.VARIABLES}}, "daily_units": {"temperature_2m_max": "°C"}}
    monkeypatch.setattr(ing.requests, "get", lambda url, params, timeout: RespuestaFalsa(200, vacia))
    with pytest.raises(ing.ErrorPermanente, match="vacía"):
        ing.ingerir(fin=date(2022, 1, 3))


def test_resumen_informa_obtenidas_y_escritas(ing):
    r = ing.ingerir_incremental(fin=date(2022, 1, 11))
    assert r["obtenidas"] == r["escritas"] == 10 and r["modo"] == "append"
    r2 = ing.ingerir_incremental(fin=date(2022, 1, 11))   # segunda ejecución: no duplica
    assert r2 == {"modo": "sin_cambios", "obtenidas": 0, "escritas": 0}
    assert _filas(ing) == (10, 10)


def test_pipeline_por_pasos_de_la_consigna(ing):
    config = ing.cargar_configuracion(fin=date(2022, 1, 4))
    registros = ing.extraer_datos(config)
    datos = ing.transformar_datos(registros)
    assert list(datos["fecha"]) == ["2022-01-01", "2022-01-02", "2022-01-03"]
    assert ing.guardar_bronze(datos, config)["escritas"] == 3
