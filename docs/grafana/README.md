# Observabilidad del pipeline con Grafana

Metabase responde preguntas sobre el clima. Grafana responde otra pregunta: **¿se puede confiar hoy en el dato?**
¿El pipeline corrió? ¿Cuánto tardó? ¿Hubo reintentos? ¿El último día cargado es ayer? ¿Falta algún día?

Es lo que ningún test de dbt detecta por sí solo. Los tests verifican el contenido de lo que se cargó, pero si el
pipeline deja de correr, los tests no corren y no avisan. Un dato viejo pasa todos los tests.

## El tablero: [`observabilidad_grupo06.json`](observabilidad_grupo06.json)

| Panel | Pregunta | Fuente | Verde cuando |
|---|---|---|---|
| Días de atraso del dato | ¿El último día cargado es ayer? | `warehouse` → `grupo06.feat_clima_diario` | atraso ≤ 1 día |
| Días faltantes en la serie | ¿Hay huecos entre el primer y el último día? | `warehouse` | 0 |
| Última corrida programada | ¿La corrida de hoy terminó bien? | `Airflow Metadata` → `dag_run` | success |
| Tareas reintentadas | ¿Hubo fallas transitorias en el período? | `Airflow Metadata` → `task_instance` | 0 |
| Duración de cada tarea por corrida | ¿Alguna tarea empezó a tardar más de lo normal? | `task_instance` + `dag_run` | estable |
| Corridas recientes | Historial: estado, tipo (programada / manual / backfill), duración, intentos | `dag_run` | — |

Usa los dos datasources que la cátedra ya tiene configurados en https://grafana.nexoacademia.org
(`airflow_meta` y `warehouse`) y filtra siempre por `pipeline_grupo06` y el schema `grupo06`.

### Valores al 17/11/2026 (últimos 30 días)
- Atraso: **1 día** (último día cargado: 16/11). Días faltantes: **0** (1.781 de 1.781).
- Última corrida programada: **success**.
- Reintentos: **3**, todos del 23/10 (corte de red en el servidor, documentado en `README_GRUPO.md`). Desde el 24/10, ninguno.
- Duración típica de una corrida: ~20 s (dbt_run ~10 s, dbt_test ~5 s, extraer_bronze ~3 s, publicar_gold ~2 s).

## Por qué es un JSON en el repo y no un tablero en el servidor
El usuario `grupo06` de Grafana tiene rol **Viewer**: puede consultar los datasources pero no puede guardar tableros.
Por eso se versiona la definición. Quien tenga rol Editor la importa en *Dashboards → New → Import → Upload JSON*
y el tablero queda igual. Es la misma lección que con Metabase: lo que está en el repositorio viaja solo; lo que no,
hay que rehacerlo a mano.

Las seis consultas se probaron contra los datasources del servidor el 17/11/2026 con el usuario `grupo06`.
