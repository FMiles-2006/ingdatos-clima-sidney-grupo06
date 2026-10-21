# Tablero "Clima de Sídney" (Metabase)

Metabase **no se versiona**: este archivo y las capturas de `docs/capturas/` son lo que viaja con el repo para rehacerlo
en el servidor (https://metabase.nexoacademia.org).

**Conexión:** base de datos PostgreSQL de la cátedra, base `warehouse`, schema **`grupo06`**.

> **Pendiente (21/10/2026):** el usuario `grupo06` de Metabase solo ve la *Sample Database*. Agregar una base de datos en
> Metabase requiere permisos de administrador, así que hay que pedirle a la cátedra que conecte `warehouse` (host `postgres`,
> puerto 5432, usuario `grupo06`, schema `grupo06`) o que habilite esa conexión para el grupo.
Tablas: `fct_clima_mensual`, `feat_clima_diario`. Filtro común sugerido: `mes_completo = verdadero` para totales.

**Decisión que apoya el tablero:** ¿conviene planificar actividades al aire libre en Sídney en un mes dado? Por ejemplo,
para un evento o la temporada de un negocio: qué meses son más lluviosos, más calurosos o más ventosos, y si el mes actual
viene más cálido o más húmedo que lo normal.

## 1. "¿El mes fue más cálido o más frío de lo normal?"
- **Tipo:** barras. **Tabla:** `fct_clima_mensual`.
- **Eje X:** `mes` (por mes). **Eje Y:** `anomalia_temperatura_c`.
- **Título:** *Anomalía de temperatura mensual (°C respecto del promedio 2022-2025)*.
- Colores: positivo cálido, negativo frío (Metabase: *Display → Show values* y color por signo).
- **Lectura:** barras arriba de 0 = mes más cálido que lo habitual para esa época.

## 2. "¿Cuánto llovió cada mes comparado con lo normal?"
- **Tipo:** combo (barras + línea). **Tabla:** `fct_clima_mensual`, filtro `mes_completo = true`.
- **Eje X:** `mes`. **Barras:** `lluvia_total_mm`. **Línea:** `lluvia_referencia_mm`.
- **Título:** *Lluvia mensual vs. lluvia normal del mes (mm)*.
- **Lectura:** barra por encima de la línea = mes más lluvioso que lo habitual.

## 3. "¿Qué estaciones tienen más días de lluvia y de calor?"
- **Tipo:** barras agrupadas. **Tabla:** `fct_clima_mensual`, filtro `mes_completo = true`.
- **Agrupar por:** `estacion`. **Métricas:** promedio de `dias_con_lluvia`, promedio de `dias_de_calor_30c`.
- **Título:** *Días de lluvia y días de calor (≥ 30 °C) por mes, según la estación*.
- **Lectura:** cuántos días de lluvia/calor esperar en un mes típico de cada estación.

## 4. "¿Cómo viene la temperatura en las últimas semanas?"
- **Tipo:** línea. **Tabla:** `feat_clima_diario`, filtro `fecha` = últimos 90 días.
- **Eje X:** `fecha`. **Series:** `temp_media_c` y `temp_media_movil_7d_c`.
- **Título:** *Temperatura media diaria y promedio móvil de 7 días (últimos 90 días)*.
- **Lectura:** la línea suave (7 días) muestra la tendencia sin el ruido diario; es el feature que usaría un modelo.

## 5. (opcional) Indicadores del mes actual
- **Tipo:** número. `fct_clima_mensual` filtrado al último mes: `temperatura_media_c`, `lluvia_total_mm`, `dias_con_lluvia`.
- **Título:** *Mes en curso* (aclarar que está incompleto).

## Cómo rehacerlo en el servidor (5 minutos)
1. Metabase → **+ Nuevo → Pregunta → Datos sin procesar → warehouse → Grupo06 → Fct Clima Mensual**.
2. Armar cada pregunta según la ficha, **Guardar** con el título indicado en la colección del grupo.
3. **+ Nuevo → Panel** "Clima de Sídney — grupo06", agregar las preguntas, filtro de panel por `mes` (opcional).
4. Sacar una captura de cada gráfico y del panel completo a `docs/capturas/` y commitearlas.

Todas las preguntas leen tablas **ya agregadas por dbt**: Metabase no agrega 1.754 filas en cada carga, por eso el tablero carga rápido.
