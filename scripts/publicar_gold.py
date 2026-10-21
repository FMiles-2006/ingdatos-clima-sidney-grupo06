"""
Publica la capa gold (calculada por dbt en DuckDB) en PostgreSQL, el warehouse de
serving que consulta Metabase.

Por qué se mueve la gold del lakehouse al warehouse:
  * Metabase (y cualquier usuario de negocio) se conecta a PostgreSQL, no a archivos
    Delta en MinIO ni a un archivo DuckDB que el pipeline reescribe en cada corrida.
  * PostgreSQL atiende muchas consultas concurrentes y chicas (las del tablero);
    DuckDB es un motor embebido de un solo proceso escritor.
  * Separa el "taller" (lake + dbt) de la "vidriera": si el build falla, el tablero
    sigue mostrando la última versión buena.

Idempotencia: cada tabla se reemplaza con DELETE + INSERT dentro de UNA transacción.
  * Correrlo dos veces deja exactamente el mismo resultado (no duplica).
  * Es atómico: Metabase ve la versión anterior completa o la nueva completa, nunca
    una tabla a medio cargar.
  * No se hace DROP TABLE: Metabase guarda sus preguntas atadas al id de la tabla, y
    recrearla en cada corrida rompería el tablero.

Estructura:  leer_gold -> publicar_resultado ; publicar_gold() las encadena (tarea de Airflow).
Las credenciales de PostgreSQL vienen del .env / entorno (PGHOST, PGUSER, PGPASSWORD...).

Uso:
    python -m scripts.publicar_gold
"""
from __future__ import annotations

import logging
import os
import sys

import duckdb
from sqlalchemy import create_engine, inspect, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from entorno import DUCKDB_PATH, PG_SCHEMA, pg_url  # noqa: E402

log = logging.getLogger("publicar_gold")

# tabla gold (modelo dbt en models/marts) -> clave primaria
TABLAS = {
    "fct_clima_mensual": "mes",
    "feat_clima_diario": "fecha",
}

VARIABLES_PG = ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]


class ErrorConfiguracion(RuntimeError):
    """Falta una variable obligatoria o el archivo DuckDB de dbt."""


def _validar_configuracion() -> None:
    faltan = [v for v in VARIABLES_PG if not os.environ.get(v)]
    if faltan:
        raise ErrorConfiguracion(f"Faltan variables obligatorias en el .env o el entorno: {', '.join(faltan)}")
    if not os.path.exists(DUCKDB_PATH):
        raise ErrorConfiguracion(
            f"No existe {DUCKDB_PATH}: primero hay que correr dbt run (la gold se calcula ahí).")


def leer_gold() -> dict:
    """Lee las tablas gold que construyó dbt en DuckDB. Devuelve {tabla: DataFrame}."""
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        datos = {tabla: con.sql(f"select * from {tabla}").df() for tabla in TABLAS}
    finally:
        con.close()
    for tabla, df in datos.items():
        if df.empty:
            raise RuntimeError(f"La gold '{tabla}' está vacía: no se publica para no vaciar el tablero.")
        log.info("Leídas %d filas de %s (DuckDB)", len(df), tabla)
    return datos


def _publicar_tabla(engine, tabla: str, df) -> int:
    pk = TABLAS[tabla]
    with engine.begin() as con:                       # una transacción por tabla
        insp = inspect(con)
        existe = insp.has_table(tabla, schema=PG_SCHEMA)
        if existe:
            cols_pg = {c["name"] for c in insp.get_columns(tabla, schema=PG_SCHEMA)}
            if cols_pg != set(df.columns):
                # Cambió el contrato (columna nueva/renombrada): se recrea y se avisa.
                log.warning("Cambió el esquema de %s: se recrea la tabla (revisar el tablero)", tabla)
                con.execute(text(f'drop table "{PG_SCHEMA}"."{tabla}"'))
                existe = False
        if not existe:
            df.head(0).to_sql(tabla, con, schema=PG_SCHEMA, index=False)
            con.execute(text(f'alter table "{PG_SCHEMA}"."{tabla}" add primary key ("{pk}")'))
        con.execute(text(f'delete from "{PG_SCHEMA}"."{tabla}"'))
        df.to_sql(tabla, con, schema=PG_SCHEMA, index=False, if_exists="append",
                  method="multi", chunksize=1000)
        n = con.execute(text(f'select count(*) from "{PG_SCHEMA}"."{tabla}"')).scalar()
    if n != len(df):
        raise RuntimeError(f"{tabla}: se esperaban {len(df)} filas y quedaron {n}")
    log.info("Publicadas %d filas en %s.%s", n, PG_SCHEMA, tabla)
    return n


def publicar_resultado(datos: dict) -> dict:
    """Reemplaza cada tabla en PostgreSQL (DELETE + INSERT, una transacción por tabla)."""
    engine = create_engine(pg_url(), pool_pre_ping=True)
    return {tabla: _publicar_tabla(engine, tabla, df) for tabla, df in datos.items()}


def publicar_gold() -> dict:
    _validar_configuracion()
    resultado = publicar_resultado(leer_gold())
    log.info("RESUMEN: %s", ", ".join(f"{t}={n} filas" for t, n in resultado.items()))
    return resultado


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        publicar_gold()
    except ErrorConfiguracion as e:
        log.error("CONFIGURACIÓN INVÁLIDA: %s", e)
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
