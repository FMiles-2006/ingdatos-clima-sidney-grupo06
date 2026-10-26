"""
Configura Metabase (local) por API: admin, conexión al warehouse, preguntas y tablero.

Por qué un script y no clics: Metabase NO se versiona. Si el tablero se arma a mano,
no viaja con el repositorio y hay que rehacerlo en cada máquina o servidor. Con este
script el tablero queda definido como código (mismas preguntas, mismos títulos) y se
reconstruye igual en cualquier lado.

Es idempotente: si el admin, la conexión, las preguntas o el tablero ya existen, los
actualiza en vez de duplicarlos.

Variables de entorno: MB_URL, MB_ADMIN_EMAIL, MB_ADMIN_PASSWORD, PGHOST, PGPORT,
PGDATABASE, PGUSER, PGPASSWORD, PGSCHEMA (las define local/docker-compose.yml).
"""
from __future__ import annotations

import os
import sys
import time

import requests

MB = os.environ.get("MB_URL", "http://localhost:3000").rstrip("/")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@grupo06.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "ClimaSidney-2026!")
SCHEMA = os.environ.get("PGSCHEMA", "grupo06")

NOMBRE_BASE = "Warehouse local (grupo06)"
NOMBRE_COLECCION = "Clima de Sídney · grupo06"
NOMBRE_TABLERO = "Clima de Sídney"

S = requests.Session()


def api(metodo: str, ruta: str, **kw):
    r = S.request(metodo, f"{MB}/api{ruta}", timeout=60, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{metodo} {ruta} -> HTTP {r.status_code}: {r.text[:500]}")
    return r.json() if r.text else None


# ---------------------------------------------------------------------------
# 1. Esperar a Metabase, crear el admin (primera vez) e iniciar sesión
# ---------------------------------------------------------------------------
def esperar_metabase(segundos: int = 300) -> None:
    limite = time.time() + segundos
    while time.time() < limite:
        try:
            if requests.get(f"{MB}/api/health", timeout=5).json().get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(5)
    raise RuntimeError(f"Metabase no respondió en {segundos} s en {MB}")


def iniciar_sesion() -> None:
    props = api("GET", "/session/properties")
    if not props.get("has-user-setup"):
        print("· Primera vez: creando el usuario administrador")
        api("POST", "/setup", json={
            "token": props["setup-token"],
            "user": {"first_name": "Admin", "last_name": "grupo06", "email": ADMIN_EMAIL,
                     "password": ADMIN_PASSWORD, "site_name": "Clima de Sídney · grupo06"},
            "prefs": {"site_name": "Clima de Sídney · grupo06", "site_locale": "es",
                      "allow_tracking": False},
        })
    sesion = api("POST", "/session", json={"username": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    S.headers["X-Metabase-Session"] = sesion["id"]
    print(f"· Sesión iniciada como {ADMIN_EMAIL}")


# ---------------------------------------------------------------------------
# 2. Conexión al warehouse (PostgreSQL local, solo el schema del grupo)
# ---------------------------------------------------------------------------
def conectar_warehouse() -> int:
    detalles = {
        "host": os.environ.get("PGHOST", "postgres"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "warehouse"),
        "user": os.environ.get("PGUSER", "grupo06"),
        "password": os.environ.get("PGPASSWORD", ""),
        "ssl": False,
        "schema-filters-type": "inclusion",
        "schema-filters-patterns": SCHEMA,
    }
    bases = api("GET", "/database")
    bases = bases.get("data", bases) if isinstance(bases, dict) else bases
    existente = next((b for b in bases if b["name"] == NOMBRE_BASE), None)
    if existente:
        db_id = existente["id"]
        api("PUT", f"/database/{db_id}", json={"details": detalles})
        print(f"· Conexión '{NOMBRE_BASE}' ya existía (id {db_id}): actualizada")
    else:
        db_id = api("POST", "/database", json={"engine": "postgres", "name": NOMBRE_BASE,
                                               "details": detalles, "is_full_sync": True})["id"]
        print(f"· Conexión '{NOMBRE_BASE}' creada (id {db_id})")
    api("POST", f"/database/{db_id}/sync_schema")

    # Esperar a que Metabase descubra las tablas gold
    esperadas = {"fct_clima_mensual", "feat_clima_diario"}
    for _ in range(60):
        meta = api("GET", f"/database/{db_id}/metadata")
        vistas = {t["name"] for t in meta.get("tables", []) if t.get("schema") == SCHEMA}
        if esperadas <= vistas:
            print(f"· Tablas sincronizadas: {sorted(vistas)}")
            return db_id
        time.sleep(3)
    raise RuntimeError(f"Metabase no encontró las tablas {esperadas} en el schema {SCHEMA}. "
                       "¿Corrió el servicio 'pipeline'?")


# ---------------------------------------------------------------------------
# 3. Preguntas (una por pregunta de negocio) y tablero
# ---------------------------------------------------------------------------
def preguntas(db: int) -> list[dict]:
    t_mes, t_dia = f"{SCHEMA}.fct_clima_mensual", f"{SCHEMA}.feat_clima_diario"

    def nativa(sql: str) -> dict:
        return {"type": "native", "native": {"query": sql.strip()}, "database": db}

    return [
        {
            "name": "Temperatura media del último mes completo (°C)",
            "description": "Temperatura media del último mes con todos sus días, y su diferencia con el promedio de ese mes en 2022-2025.",
            "display": "scalar",
            "dataset_query": nativa(f"""
                select temperatura_media_c as "Temperatura media (°C)"
                from {t_mes} where mes_completo order by mes desc limit 1"""),
            "visualization_settings": {"scalar.suffix": " °C"},
            "pos": {"row": 0, "col": 0, "size_x": 8, "size_y": 3},
        },
        {
            "name": "Anomalía del último mes completo (°C vs. 2022-2025)",
            "description": "Cuántos °C más cálido (+) o más frío (−) fue el último mes completo respecto de lo normal.",
            "display": "scalar",
            "dataset_query": nativa(f"""
                select anomalia_temperatura_c as "Anomalía (°C)"
                from {t_mes} where mes_completo order by mes desc limit 1"""),
            "visualization_settings": {"scalar.suffix": " °C"},
            "pos": {"row": 0, "col": 8, "size_x": 8, "size_y": 3},
        },
        {
            "name": "Lluvia del último mes completo (mm)",
            "description": "Milímetros de lluvia acumulados en el último mes completo.",
            "display": "scalar",
            "dataset_query": nativa(f"""
                select lluvia_total_mm as "Lluvia (mm)"
                from {t_mes} where mes_completo order by mes desc limit 1"""),
            "visualization_settings": {"scalar.suffix": " mm"},
            "pos": {"row": 0, "col": 16, "size_x": 8, "size_y": 3},
        },
        {
            "name": "¿El mes fue más cálido o más frío de lo normal?",
            "description": "Anomalía de temperatura mensual: °C por encima (+) o por debajo (−) del promedio de ese mismo mes en 2022-2025.",
            "display": "bar",
            "dataset_query": nativa(f"""
                select mes as "Mes", anomalia_temperatura_c as "Anomalía de temperatura (°C)"
                from {t_mes} order by mes"""),
            "visualization_settings": {
                "graph.dimensions": ["Mes"], "graph.metrics": ["Anomalía de temperatura (°C)"],
                "graph.x_axis.title_text": "Mes", "graph.y_axis.title_text": "°C respecto de lo normal",
            },
            "pos": {"row": 3, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "¿Cuánto llovió cada mes comparado con lo normal?",
            "description": "Lluvia del mes (barras) frente a la lluvia promedio de ese mismo mes en 2022-2025 (línea). Solo meses completos.",
            "display": "combo",
            "dataset_query": nativa(f"""
                select mes as "Mes", lluvia_total_mm as "Lluvia del mes (mm)",
                       lluvia_referencia_mm as "Lluvia normal del mes (mm)"
                from {t_mes} where mes_completo order by mes"""),
            "visualization_settings": {
                "graph.dimensions": ["Mes"],
                "graph.metrics": ["Lluvia del mes (mm)", "Lluvia normal del mes (mm)"],
                "series_settings": {"Lluvia del mes (mm)": {"display": "bar"},
                                    "Lluvia normal del mes (mm)": {"display": "line"}},
                "graph.y_axis.title_text": "mm",
            },
            "pos": {"row": 3, "col": 12, "size_x": 12, "size_y": 7},
        },
        {
            "name": "¿Qué estaciones tienen más días de lluvia y de calor?",
            "description": "Promedio de días con lluvia (≥ 1 mm) y de días con máxima ≥ 30 °C en un mes típico de cada estación. Solo meses completos.",
            "display": "bar",
            "dataset_query": nativa(f"""
                select estacion as "Estación",
                       round(avg(dias_con_lluvia)::numeric, 1)   as "Días de lluvia por mes",
                       round(avg(dias_de_calor_30c)::numeric, 1) as "Días de calor (≥ 30 °C) por mes"
                from {t_mes} where mes_completo
                group by estacion
                order by case estacion when 'Verano' then 1 when 'Otoño' then 2
                                       when 'Invierno' then 3 else 4 end"""),
            "visualization_settings": {
                "graph.dimensions": ["Estación"],
                "graph.metrics": ["Días de lluvia por mes", "Días de calor (≥ 30 °C) por mes"],
                "graph.show_values": True,
            },
            "pos": {"row": 10, "col": 0, "size_x": 12, "size_y": 7},
        },
        {
            "name": "¿Cómo viene la temperatura en los últimos 90 días?",
            "description": "Temperatura media diaria y su promedio móvil de 7 días (feature de feat_clima_diario: solo usa días pasados).",
            "display": "line",
            "dataset_query": nativa(f"""
                select fecha as "Fecha", temp_media_c as "Temperatura media diaria (°C)",
                       temp_media_movil_7d_c as "Promedio últimos 7 días (°C)"
                from {t_dia}
                where fecha > (select max(fecha) from {t_dia}) - interval '90 days'
                order by fecha"""),
            "visualization_settings": {
                "graph.dimensions": ["Fecha"],
                "graph.metrics": ["Temperatura media diaria (°C)", "Promedio últimos 7 días (°C)"],
                "graph.y_axis.title_text": "°C",
            },
            "pos": {"row": 10, "col": 12, "size_x": 12, "size_y": 7},
        },
    ]


def coleccion() -> int:
    for c in api("GET", "/collection"):
        if c.get("name") == NOMBRE_COLECCION and not c.get("archived"):
            return c["id"]
    return api("POST", "/collection", json={"name": NOMBRE_COLECCION, "color": "#509EE3",
                                            "description": "Tablero del proyecto integrador (grupo06)."})["id"]


def guardar_preguntas(db: int, col: int) -> list[tuple[int, dict]]:
    existentes = {c["name"]: c["id"] for c in api("GET", "/card")
                  if c.get("collection_id") == col and not c.get("archived")}
    guardadas = []
    for p in preguntas(db):
        cuerpo = {k: p[k] for k in ("name", "description", "display", "dataset_query", "visualization_settings")}
        cuerpo["collection_id"] = col
        if p["name"] in existentes:
            card_id = existentes[p["name"]]
            api("PUT", f"/card/{card_id}", json=cuerpo)
        else:
            card_id = api("POST", "/card", json=cuerpo)["id"]
        # Ejecutarla una vez: si el SQL estuviera mal, fallamos acá y no en el tablero
        res = api("POST", f"/card/{card_id}/query")
        if res.get("status") != "completed":
            raise RuntimeError(f"La pregunta '{p['name']}' falló: {res.get('error')}")
        print(f"· Pregunta lista ({res['row_count']} filas): {p['name']}")
        guardadas.append((card_id, p["pos"]))
    return guardadas


def armar_tablero(col: int, tarjetas: list[tuple[int, dict]]) -> int:
    tableros = [d for d in api("GET", "/dashboard")
                if d.get("name") == NOMBRE_TABLERO and d.get("collection_id") == col and not d.get("archived")]
    if tableros:
        dash_id = tableros[0]["id"]
    else:
        dash_id = api("POST", "/dashboard", json={
            "name": NOMBRE_TABLERO, "collection_id": col,
            "description": "¿Cómo fue el clima de Sídney cada mes y cuánto se apartó de lo normal? "
                           "Fuente: Open-Meteo (ERA5 hasta 2025, Historical Forecast desde 2026)."})["id"]
    dashcards = [{"id": -(i + 1), "card_id": cid, "row": pos["row"], "col": pos["col"],
                  "size_x": pos["size_x"], "size_y": pos["size_y"],
                  "parameter_mappings": [], "visualization_settings": {}}
                 for i, (cid, pos) in enumerate(tarjetas)]
    api("PUT", f"/dashboard/{dash_id}", json={"dashcards": dashcards})
    return dash_id


def main() -> int:
    esperar_metabase()
    iniciar_sesion()
    db = conectar_warehouse()
    col = coleccion()
    tarjetas = guardar_preguntas(db, col)
    dash = armar_tablero(col, tarjetas)
    print(f"\nListo. Tablero: http://localhost:3000/dashboard/{dash}")
    print(f"Usuario: {ADMIN_EMAIL}  (contraseña: la de MB_ADMIN_PASSWORD en local/.env o la de ejemplo del README)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # mensaje claro para quien lo corre, sin traceback largo
        print(f"ERROR configurando Metabase: {e}", file=sys.stderr)
        sys.exit(1)
