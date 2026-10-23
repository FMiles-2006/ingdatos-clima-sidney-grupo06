# Entrega 4 · Serving, features y governance

Publicación: [`scripts/publicar_gold.py`](../scripts/publicar_gold.py) · Features: [`dbt/models/marts/feat_clima_diario.sql`](../dbt/models/marts/feat_clima_diario.sql) ·
Tablero: [`tablero_metabase.md`](tablero_metabase.md) · Governance: [`governance.md`](governance.md) · Notebook: [`NB06`](../notebooks/NB06_semana14_serving_metabase.ipynb)

## 1. Gold en PostgreSQL

`publicar_gold` es la **cuarta tarea del DAG**: el pipeline termina en el warehouse, nadie tiene que apretar un botón.
Publica en la base `warehouse`, schema `grupo06`:

| Tabla | Filas | Clave |
|---|---|---|
| `fct_clima_mensual` | 1 por mes (58 al 20/10/2026) | `mes` |
| `feat_clima_diario` | 1 por día (1.754) | `fecha` |

**Repetible:** cada tabla se reemplaza con `DELETE` + `INSERT` dentro de **una transacción** (correrla dos veces deja lo
mismo, verificado en NB06). Es atómica: Metabase ve la versión anterior completa o la nueva completa. No se hace `DROP TABLE`,
porque Metabase ata sus preguntas al id de la tabla y recrearla rompería el tablero. Si la gold viniera vacía, no se publica.

**¿Por qué mover la gold del lakehouse al warehouse?** Metabase y los usuarios de negocio se conectan a una base SQL, no a
archivos Delta en MinIO. PostgreSQL atiende muchas consultas chicas y concurrentes (las del tablero); DuckDB es un motor
embebido con un solo escritor, y el archivo lo reescribe el pipeline en cada corrida. Además separa el taller de la
vidriera: si un build falla, el tablero sigue mostrando la última versión buena.

Consultar desde afuera de Python (demo): en Metabase → *Nueva → Consulta SQL*, o con `psql`:
```sql
select mes, temperatura_media_c, anomalia_temperatura_c, lluvia_total_mm
from grupo06.fct_clima_mensual order by mes desc limit 12;
```

## 2. Tablero

En https://metabase.nexoacademia.org, con 4 visualizaciones; cada una responde una pregunta que se puede decir en voz alta.
Las preguntas, títulos y configuración exacta están en [`tablero_metabase.md`](tablero_metabase.md) y las capturas en `docs/capturas/`
(Metabase no se versiona: eso es lo que viaja con el repo para rehacerlo en el servidor).

## 3. Features

`feat_clima_diario` es la tabla de features: un modelo dbt **versionado, con tests, definido una sola vez**.

| Feature | Definición |
|---|---|
| `temp_media_movil_7d_c` / `_30d_c` | Promedio de la temperatura media de los últimos 7 / 30 días calendario, incluido el día |
| `lluvia_acumulada_7d_mm` / `_30d_mm` | Lluvia total de los últimos 7 / 30 días |
| `variacion_temp_vs_dia_anterior_c` | Temperatura media del día menos la del día anterior |
| `desvio_vs_media_30d_c` | Temperatura del día menos su promedio de 30 días |
| `amplitud_termica_c` | Máxima menos mínima del día |
| `dias_desde_ultima_lluvia` | Días desde el último día con ≥ 1 mm (0 = llovió hoy) |
| `dias_en_ventana_7d` | Cuántos días con datos tiene la ventana (completitud) |

**Sin fuga temporal:** cada fila describe lo que se sabe **al cierre del día `fecha`** (para predecir el día siguiente).
Todas las ventanas son `RANGE BETWEEN INTERVAL n DAYS PRECEDING AND CURRENT ROW`: solo pasado y el propio día, nunca días
posteriores. Son por **calendario** y no por cantidad de filas, así un hueco en la serie no estira la ventana hacia atrás.
El test `assert_features_sin_fuga_temporal` recalcula el promedio de 7 días usando solo fechas ≤ la del feature y
verifica que coincidan. Un "promedio del mes" asignado a cada día del mes sí tendría fuga (el día 15 usaría el 16 al 31):
por eso ese tipo de agregado vive en `fct_clima_mensual`, que es descriptivo, y no en la tabla de features.

**Reproducible:** se recalcula entera en cada `dbt build` a partir de silver: mismo bronze ⇒ mismos features.

## 4. Governance

[`governance.md`](governance.md), con las seis secciones: identificación, origen y linaje, diccionario de datos,
clasificación de sensibilidad, calidad y garantías, licencia y uso.
