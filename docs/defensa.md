# Guía de la defensa (20 minutos, en el servidor de la cátedra)

## Checklist del día anterior
- [ ] NB00 corrido (repo actualizado con `git pull`, `.env` presente).
- [ ] `pipeline_grupo06` activo en Airflow y con corridas en verde en el historial.
- [ ] Tablero rehecho en Metabase (ver `tablero_metabase.md`).
- [ ] CI en verde en GitHub; rama `demo/ci-rojo` preparada (ver abajo).
- [ ] Cada integrante puede explicar **cualquier** parte (repasar la sección "Preguntas" juntos).

## Los 10 minutos de demo (orden sugerido)

| Min | Quién | Qué se muestra | Dónde |
|---|---|---|---|
| 0-1 | A | Arquitectura en una frase + el DAG con su grafo e historial | Airflow → `pipeline_grupo06` → Grid |
| 1-3 | A | **Reejecución sin duplicar**: correr la ingesta dos veces | Terminal Jupyter: `cd ~/work/dags && python -m scripts.ingesta` (×2) → "Nada nuevo para pedir" |
| 3-5 | B | **Backfill en vivo**: *Trigger DAG w/ config* con el período que elija el docente, p. ej. `{"inicio": "2026-03-01", "fin": "2026-03-08"}` → log de `extraer_bronze`: `a extraer: 2026-03-01 -> 2026-03-07`, `(merge)`; el total de filas de bronze no cambia | Airflow |
| 5-7 | C | **Auditar**: tests en verde en el log de `dbt_build`; historial de versiones de bronze (time travel) en NB03; linaje (captura o `dbt docs`) | Airflow + Jupyter |
| 7-9 | D | **Que sirva**: el tablero, enunciando la pregunta de cada gráfico | Metabase |
| 9-10 | B | **CI** verde y el PR en rojo bloqueado | GitHub |

**Demo de CI en rojo** (preparar antes): crear una rama `demo/ci-rojo` donde `assert_temperaturas_coherentes` se rompa
(por ejemplo, en `ci/bronze_muestra.csv` poner una mínima de 35 con máxima de 12) y abrir un PR → el job **dbt** queda en rojo
y GitHub no deja mergear (protección de rama con checks obligatorios).

## Preguntas probables y respuestas cortas

**Ingesta**
- *¿Qué pasa si corren esto mañana? ¿Y dos veces seguidas?* — Mañana trae solo el día nuevo (watermark + 1). Dos veces seguidas: la segunda no le pide nada a la API.
- *La API se cae a mitad de la extracción.* — No se escribe nada: validamos toda la respuesta antes de escribir y Delta escribe en un commit atómico. Los fallos transitorios se reintentan; si persisten, Airflow reintenta la tarea más tarde. El watermark no avanzó, así que la próxima corrida pide lo mismo.
- *¿Por qué ese watermark?* — La API es diaria y se consulta por rango de fechas: la última fecha cargada define exactamente qué falta. Se deriva de los datos, no de un archivo que se pueda desincronizar.

**Almacenamiento**
- *¿Por qué bronze guarda crudo si después limpian?* — Para poder reprocesar sin volver a pedirle al origen y auditar qué llegó. Ejemplo real: descubrimos tarde que `rain_sum` cambia de significado entre las fuentes; como bronze lo tiene crudo, se corrigió en silver sin re-extraer.
- *Su transformación tuvo un error los últimos dos meses.* — Se corrige el SQL y se corre `dbt build`: silver y gold se reconstruyen enteras desde bronze. Si el error estaba en la ingesta: backfill de esos dos meses con config `{"inicio","fin"}` (MERGE, sin duplicar).
- *Un usuario pide que borren sus datos.* — No hay datos personales (ver governance §4). Si los hubiera: `DELETE` en Delta + `VACUUM` para eliminar los archivos viejos (el time travel los conserva hasta entonces).

**Transformación**
- *¿Por qué silver es tabla y no vista?* — La fuente entra por el plugin delta en el build; una vista quedaría apuntando a algo que no existe fuera de dbt. Son 1.800 filas: el costo es nulo. Gold es tabla porque la lee el tablero.
- *¿Qué pregunta responde la gold?* — Cómo fue cada mes y cuánto se apartó de lo normal. La usaría alguien que planifica actividades al aire libre o la temporada de un negocio.

**Orquestación**
- *¿Por qué tareas separadas?* — Fallan por motivos distintos (pip, API, test, Postgres), se reintentan distinto y se diagnostican en su log.
- *Pipeline caído una semana.* — Nada que hacer a mano: la próxima corrida usa el watermark, y `desde = min(watermark+1, inicio)` hace que pida toda la semana faltante.
- *¿Cuántos reintentos y por qué?* — 2, con 5 y 10 minutos: cubren una caída de ~15 min de la API sin esconder un problema real. dbt solo 1, porque un test que falla vuelve a fallar.
- *Si falla dbt, ¿se publica?* — No: `all_success`. El tablero sigue con la última versión válida.

**Transversales**
- *Idempotencia (E1) y backfill (E3).* — El backfill es reejecutar el pasado; solo es seguro si reejecutar no duplica. Por eso el reproceso hace MERGE. Y los reintentos de Airflow son seguros por lo mismo.
- *¿Por qué Postgres y no servir directo del lakehouse?* — Metabase habla SQL con un servidor; Postgres aguanta muchas consultas chicas concurrentes y separa el taller de la vidriera.
- *¿Qué no detectaría ningún test?* — Un valor plausible pero incorrecto de la API; que el dato quede viejo si el DAG deja de correr (no hay alerta de frescura); que el cambio de fuente en 2026 introduzca un sesgo pequeño; que la definición de "día lluvioso" no sea la que necesita el usuario.

## Cierre: qué haríamos distinto / qué quedó pendiente
- **Distinto:** pensar desde el día 1 que el Airflow es compartido y solo ve `dags/` (perdimos tiempo con rutas que no existían); armar el CI antes, no al final; pasar el intervalo de Airflow a la ingesta desde E1.
- **Pendiente:** reprocesar 2026 con ERA5 cuando se publique (hoy queda la salida del modelo); alerta de frescura (sensor/SLA o Grafana); manejar las credenciales con Connections de Airflow en vez de un `.env`; modelos dbt incrementales si el volumen creciera; un modelo que use los features.
- **Punto más débil (dicho con honestidad):** la mezcla de dos fuentes con distinta naturaleza (reanálisis vs. modelo) y una referencia de solo 4 años para las anomalías.
