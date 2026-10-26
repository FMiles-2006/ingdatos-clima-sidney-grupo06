# Metabase local (Postgres + pipeline + tablero)

En el servidor de la cátedra, la base `warehouse` todavía no está conectada a Metabase para grupo06.
Por indicación del profesor, el tablero se prueba **en local**: un Postgres y un Metabase en Docker,
cargados por **el mismo pipeline** que corre Airflow en el servidor.

```
Open-Meteo ──► scripts/ingesta.py ──► local/lake/bronze_clima_sidney (Delta)
                                              │  dbt seed + run + test (34 tests)
                                              ▼
                      PostgreSQL local: warehouse.grupo06.fct_clima_mensual / feat_clima_diario
                                              │
                                              ▼
                      Metabase local (http://localhost:3000) ── tablero "Clima de Sídney"
```

Qué cambia respecto del servidor, y por qué:

| | Servidor de la cátedra | Local |
|---|---|---|
| Bronze | Delta en MinIO (`s3://grupo06/bronze/clima_sidney`) | Delta en una carpeta (`local/lake/`); el mismo código, solo cambia `BRONZE_PATH` |
| Quién corre el pipeline | Airflow, todos los días a las 06:00 | El servicio `pipeline`, cuando se lo lanza |
| Warehouse | Postgres de la cátedra, base `warehouse`, schema `grupo06` | Postgres en Docker, **misma base, schema y tablas** |
| Tablero | Pendiente de que la cátedra conecte la base | Se arma solo con `configurar_metabase.py` |

## Requisitos
- **Docker Desktop** abierto (Windows/Mac) o Docker Engine con el plugin compose (Linux).
- Unos 3 GB de RAM libres. La primera vez baja las imágenes (~1 GB) y las librerías del pipeline.
- Internet (la ingesta le pide los datos a Open-Meteo).

## Puesta en marcha (3 comandos)

```bash
git clone https://github.com/FMiles-2006/ingdatos-clima-sidney-grupo06.git
cd ingdatos-clima-sidney-grupo06/local
docker compose up -d
```

`docker compose up -d` hace todo en orden:
1. **postgres**: crea la base `warehouse`, el schema `grupo06` y la base interna de Metabase.
2. **pipeline**: trae de Open-Meteo 2022-01-01 → ayer, corre dbt (seed, run, test) y publica la gold. Tarda de 2 a 4 minutos.
3. **metabase**: arranca en http://localhost:3000. La primera vez tarda 1-2 minutos.
4. **configurar_metabase**: crea el admin, la conexión, las 7 preguntas y el tablero.

Para seguir el avance: `docker compose logs -f pipeline configurar_metabase`. Al final aparece:
```
Listo. Tablero: http://localhost:3000/dashboard/1
```

**Ingreso a Metabase:** usuario `admin@grupo06.local`, contraseña `ClimaSidney-2026!`.
Son credenciales de ejemplo, solo para este entorno local. Para cambiarlas, copiar `.env.example` a `.env` en esta carpeta, editarlo **antes** del primer `up` y volver a levantar.

## El tablero "Clima de Sídney"

| # | Pregunta | Tipo | Tabla |
|---|---|---|---|
| 1-3 | Último mes completo: temperatura media, anomalía y lluvia | Números | `fct_clima_mensual` |
| 4 | ¿El mes fue más cálido o más frío de lo normal? | Barras de anomalía (°C vs. 2022-2025) | `fct_clima_mensual` |
| 5 | ¿Cuánto llovió cada mes comparado con lo normal? | Barras (lluvia) + línea (lluvia normal) | `fct_clima_mensual` |
| 6 | ¿Qué estaciones tienen más días de lluvia y de calor? | Barras agrupadas por estación | `fct_clima_mensual` |
| 7 | ¿Cómo viene la temperatura en los últimos 90 días? | Línea diaria + promedio móvil de 7 días | `feat_clima_diario` |

**Decisión que apoya:** planificar actividades al aire libre o la temporada de un negocio en Sídney: qué meses llueven más,
cuáles son más calurosos, y si el mes actual viene distinto de lo normal.

Todas las preguntas leen tablas **ya agregadas por dbt**: Metabase no hace el trabajo pesado, por eso el tablero carga rápido.

## Uso diario y demos

```bash
docker compose run --rm pipeline              # actualizar: solo trae los días nuevos (watermark)
docker compose run --rm pipeline              # otra vez: "Nada nuevo para pedir" → idempotencia
docker compose run --rm --no-deps configurar_metabase   # rehacer el tablero (no duplica)
docker compose down                           # apagar (conserva datos y tablero)
docker compose down -v && rm -rf lake         # borrar todo y empezar de cero
```

Consultar la gold **desde afuera de Python** (lo pide la demo de E4), con cualquier cliente SQL (DBeaver, pgAdmin, psql):
host `localhost`, puerto **5433**, base `warehouse`, usuario `grupo06`, contraseña `grupo06_local`.
```sql
select mes, temperatura_media_c, anomalia_temperatura_c, lluvia_total_mm
from grupo06.fct_clima_mensual order by mes desc limit 12;
```

## Por qué está armado así
- **Mismo código que producción.** El servicio `pipeline` monta el repositorio y corre `scripts/ingesta.py`, dbt y `scripts/publicar_gold.py` sin cambios. Solo cambian variables de entorno (`BRONZE_PATH`, `PG*`). Lo que se ve en el tablero local es lo que produciría el pipeline del servidor.
- **El tablero como código.** Metabase no se versiona; `configurar_metabase.py` define las preguntas y el panel por API, así que el tablero se reconstruye igual en cualquier máquina. Cuando la cátedra conecte la base del servidor, el mismo script sirve apuntando `MB_URL` y las variables `PG*` al servidor.
- **Puertos 5433 y 3000.** 5433 evita chocar con un Postgres que ya tengan instalado.
- **Verificado en CI.** El workflow `Metabase local` (`.github/workflows/metabase-local.yml`) levanta este mismo entorno en GitHub Actions, corre el pipeline dos veces (la segunda no trae nada), reconfigura el tablero (no duplica) y guarda la captura en `docs/capturas/metabase_tablero.png`.

## Problemas comunes

| Síntoma | Causa | Solución |
|---|---|---|
| `port is already allocated` (3000 o 5433) | Otro programa usa el puerto | Cambiar el puerto de la izquierda en `docker-compose.yml` (ej. `"3001:3000"`) |
| `configurar_metabase` termina con error y `pipeline` falló | Sin internet o Open-Meteo caído | `docker compose logs pipeline`; reintentar con `docker compose up -d` |
| Metabase tarda mucho la primera vez | Inicializa su base interna | Esperar 1-2 minutos; `docker compose logs -f metabase` |
| Cambié la contraseña en `.env` y no funciona | Postgres ya se había inicializado con la anterior | `docker compose down -v` y volver a levantar |
