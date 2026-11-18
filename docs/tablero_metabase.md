# Tablero "Clima de Sídney — grupo06" (Metabase)

**Dónde:** https://metabase.nexoacademia.org → colección **Grupo06 · Clima de Sídney** → panel *Clima de Sídney — grupo06*.
**Datos:** base `warehouse`, schema `grupo06`, tablas `fct_clima_mensual` y `feat_clima_diario` (gold publicada por el DAG).
**Datos:** las mediciones salen todas de Open-Meteo a través de nuestro pipeline. La única referencia externa es la
fase de El Niño / La Niña de cada mes (`fase_enso`), que entra al pipeline como seed de dbt (ver más abajo).

Metabase **no se versiona**: este archivo (pregunta, SQL y lectura de cada gráfico) y las capturas son lo que viaja con
el repositorio para poder rehacerlo. Cada gráfico es una *pregunta* guardada en la colección; el panel las ordena.

![Tablero, parte 1](capturas/metabase_tablero_1.png)
![Tablero, parte 2](capturas/metabase_tablero_2.png)
![Tablero, parte 3](capturas/metabase_tablero_3.png)
![Tablero, parte 4](capturas/metabase_tablero_4.png)

## La pregunta que ordena el tablero

**¿Cómo afecta la lluvia a la temperatura en Sídney?** Y, como contexto: ¿este año viene más cálido o más lluvioso que
los anteriores?

**Respuesta corta (datos 2022-01-01 → ayer):**
- La lluvia **casi no cambia la temperatura media** del día (en verano los días de lluvia son ~1 °C más frescos; en el
  resto del año la diferencia es de 0,5 °C o menos).
- Lo que sí cambia es la **amplitud térmica**: un día de lluvia tiene ~3 °C menos de diferencia entre máxima y mínima
  (las nubes frenan el calentamiento de día y el enfriamiento de noche).
- Cuantos más días pasan sin llover, más crece la amplitud (de 6,2 °C a ~10 °C) y más se calienta el día respecto de su
  media de 30 días.
- En lo que va del año, 2026 es el más cálido de la serie (comparando todos los años en el mismo tramo 1/1 → ayer).

**Qué decisión apoya:** quien planifica en Sídney algo sensible a la temperatura (eventos al aire libre, riego,
climatización) puede esperar días más extremos tras una racha seca y días más parejos con lluvia, y sabe si el año en
curso viene por encima o por debajo de lo normal.

## Los gráficos

| # | Título en Metabase | Pregunta que responde | Se actualiza |
|---|---|---|---|
| 1–3 | Último día cargado · Temperatura media del último día · Lluvia de los últimos 30 días | ¿Hasta qué día hay datos y cómo fue ese día? | cada corrida |
| 4 | ¿Los días de lluvia son más frescos? Temperatura media por estación | ¿Cambia la temperatura media en días con lluvia vs. secos? | cada corrida |
| 5 | ¿Cuánto se achica la amplitud térmica (máxima − mínima) cuando llueve? | ¿Cambia la diferencia entre máxima y mínima? | cada corrida |
| 6 | Cuantos más días sin llover, ¿más calor y más amplitud? | ¿Las rachas secas traen días más extremos? | cada corrida |
| 7 | Temperatura media de cada mes, año por año (mes en curso: parcial) | ¿Cada mes de este año está arriba o abajo de los anteriores? | cada corrida |
| 8 | ¿Este año viene más cálido? (1/1 → último día cargado, vs. 2022–2025) | ¿Cuánto se aparta cada año de lo normal, en el mismo tramo de días? | cada corrida |
| 9 | Lluvia acumulada por año (1/1 → último día cargado) | ¿Este año llovió más o menos, en el mismo tramo de días? | cada corrida |
| 10 | Lluvia de los últimos 30 días a lo largo del año, año por año | ¿Todos los años llueve en la misma época? | cada corrida |
| 11 | Temperatura según la fase de El Niño / La Niña | En meses de La Niña / El Niño, ¿hace más frío o más calor que lo habitual? | al cerrar cada mes |
| 12 | Lluvia según la fase de El Niño / La Niña | En meses de La Niña / El Niño, ¿llueve más o menos que lo habitual? | al cerrar cada mes |
| 13 | Temperatura de cada mes vs. su promedio 2022–2025, según la fase | ¿Qué meses fueron más cálidos o fríos, y en qué fase estaba el Pacífico? | al cerrar cada mes |
| 14 | Lluvia de cada mes vs. su promedio 2022–2025, según la fase | ¿Qué meses fueron más lluviosos o secos, y en qué fase? | al cerrar cada mes |

Colores fijos: años (gráficos 7 y 10) 2022 azul, 2023 verde, 2024 amarillo, 2025 naranja, 2026 rosa; fases (11 a 14)
**La Niña rosa, Neutral gris, El Niño celeste**.

Notas de lectura:
- **Día con lluvia** = `dias_desde_ultima_lluvia = 0`: ese día cayó ≥ 1 mm.
- **Amplitud térmica** = máxima − mínima del día. No es una temperatura: por eso en verano (días nublados y húmedos)
  puede ser menor que en invierno.
- **"Lo normal" / "su promedio"** = el promedio de ese mismo mes calendario en 2022–2025. En los gráficos 11 a 14 el 0 es
  ese promedio, que cambia de mes a mes (enero ~22 °C, julio ~12 °C): por eso se muestra la diferencia y no la
  temperatura absoluta.
- Los gráficos 8 y 9 comparan **el mismo tramo de días** en todos los años (del 1/1 al último día cargado). Ningún año
  incluye el verano de diciembre, así que 2026 compite en igualdad de condiciones.
- **Límites:** cinco años alcanzan para describir, no para hablar de cambio climático (~30 años). 2026 viene de otra API
  (Historical Forecast en lugar de ERA5), lo que puede introducir un sesgo. Ver la interpretación ENSO más abajo.

### El Niño / La Niña: qué es la columna `fase_enso` y de dónde sale
- **Qué es:** la fase del fenómeno El Niño-Oscilación del Sur vigente cada mes: La Niña (en el este de Australia suele
  traer más lluvia y días más frescos), El Niño (menos lluvia y más calor) o Neutral.
- **De dónde sale:** de las declaraciones de la Oficina de Meteorología de Australia (BoM). Es la única referencia
  externa del pipeline: un CSV de 9 filas (`dbt/seeds/fases_enso.csv`), con la declaración que justifica cada período.
  Regla: un mes toma la fase vigente el día 15.
- **En qué capa entra:** seed → **silver** `stg_fases_enso` (tipos + test propio: los períodos no se pisan) → **gold**
  `fct_clima_mensual.fase_enso` (join por mes; `accepted_values`; 'Sin clasificar' si el seed no cubre el mes).
- **Qué muestra (meses completos 2022 → oct-2026):** La Niña −0,48 °C y +19 % de lluvia; El Niño +0,28 °C y −18 %;
  Neutral +0,31 °C y −14 %. La Niña se distingue con claridad; El Niño se parece a Neutral en temperatura pero es el más
  seco. Es una **coincidencia consistente** con lo esperado, no una prueba: cinco años no alcanzan para atribuir causa, y
  "lo normal" (2022–2025) incluye muchos meses de La Niña, lo que corre la referencia hacia lo fresco y lluvioso.

## SQL de cada gráfico (para rehacerlo: *+ Nuevo → Consulta SQL → warehouse*)

**1–3 · Números**
```sql
select to_char(max(fecha), 'DD/MM/YYYY') as "Último día cargado" from grupo06.feat_clima_diario;
select temp_media_c as "Temperatura media (°C)" from grupo06.feat_clima_diario order by fecha desc limit 1;
select lluvia_acumulada_30d_mm as "Lluvia 30 días (mm)" from grupo06.feat_clima_diario order by fecha desc limit 1;
```

**4 · Barras agrupadas**
```sql
with d as (
  select case when extract(month from fecha) in (12,1,2) then 1 when extract(month from fecha) in (3,4,5) then 2
              when extract(month from fecha) in (6,7,8) then 3 else 4 end as orden,
         dias_desde_ultima_lluvia = 0 as llovio,
         temp_media_c
  from grupo06.feat_clima_diario
  where dias_desde_ultima_lluvia is not null
)
select (array['Verano','Otoño','Invierno','Primavera'])[orden] as "Estación",
       round(avg(temp_media_c) filter (where not llovio)::numeric, 1) as "Día seco",
       round(avg(temp_media_c) filter (where llovio)::numeric, 1) as "Día con lluvia",
       round(avg(temp_media_c)::numeric, 1) as "Promedio de la estación"
from d group by orden order by orden
```

**5 · Barras agrupadas** (X = Estación, serie = Condición)
```sql
with d as (
  select case when extract(month from fecha) in (12,1,2) then 1 when extract(month from fecha) in (3,4,5) then 2
              when extract(month from fecha) in (6,7,8) then 3 else 4 end as orden,
         case when dias_desde_ultima_lluvia = 0 then 'Día con lluvia' else 'Día seco' end as condicion,
         amplitud_termica_c
  from grupo06.feat_clima_diario
  where dias_desde_ultima_lluvia is not null
)
select (array['Verano','Otoño','Invierno','Primavera'])[orden] as "Estación", condicion as "Condición",
       round(avg(amplitud_termica_c)::numeric, 1) as "Amplitud térmica promedio (°C)"
from d group by orden, condicion order by orden, condicion
```

**6 · Combo** (barras = amplitud; línea en eje derecho = temperatura vs. media de 30 días)
```sql
select case when dias_desde_ultima_lluvia >= 7 then '7 o más' else dias_desde_ultima_lluvia::text end as "Días desde la última lluvia",
       round(avg(amplitud_termica_c)::numeric, 1) as "Amplitud térmica promedio (°C)",
       round(avg(desvio_vs_media_30d_c)::numeric, 2) as "Temperatura vs. media de 30 días (°C)"
from grupo06.feat_clima_diario
where dias_desde_ultima_lluvia is not null
group by 1, least(dias_desde_ultima_lluvia, 7)
order by least(dias_desde_ultima_lluvia, 7)
```

**7 · Líneas** (X = Mes, serie = Año)
```sql
select (array['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic'])[mes_numero] as "Mes",
       anio::text as "Año", temperatura_media_c as "Temperatura media (°C)"
from grupo06.fct_clima_mensual
where mes_completo or mes = (select max(mes) from grupo06.fct_clima_mensual)
order by mes_numero, anio
```

**8 · Barras apiladas** (dos series para colorear por signo)
```sql
with u as (select max(fecha) as ultimo from grupo06.feat_clima_diario),
por_anio as (
  select extract(year from f.fecha)::int as anio, avg(f.temp_media_c) as temp
  from grupo06.feat_clima_diario f, u
  where to_char(f.fecha, 'MMDD') <= to_char(u.ultimo, 'MMDD')
  group by 1
),
ref as (select avg(temp) as temp_ref from por_anio where anio between 2022 and 2025)
select p.anio::text as "Año",
       case when p.temp - r.temp_ref > 0 then round((p.temp - r.temp_ref)::numeric, 2) end as "Más cálido (°C)",
       case when p.temp - r.temp_ref <= 0 then round((p.temp - r.temp_ref)::numeric, 2) end as "Más frío (°C)"
from por_anio p, ref r order by p.anio
```

**9 · Barras**
```sql
with u as (select max(fecha) as ultimo from grupo06.feat_clima_diario)
select extract(year from f.fecha)::int::text as "Año", round(sum(f.lluvia_mm)::numeric, 0) as "Lluvia (mm)"
from grupo06.feat_clima_diario f, u
where to_char(f.fecha, 'MMDD') <= to_char(u.ultimo, 'MMDD')
group by 1 order by 1
```

**10 · Líneas** (X = mes del año en decimal, serie = Año)
```sql
select round((extract(month from fecha) + (extract(day from fecha) - 1)
             / extract(day from (date_trunc('month', fecha) + interval '1 month - 1 day')))::numeric, 3) as "Mes",
       extract(year from fecha)::int::text as "Año",
       lluvia_acumulada_30d_mm as "Lluvia de los últimos 30 días (mm)"
from grupo06.feat_clima_diario
where fecha >= date '2022-01-30'      -- antes no hay 30 días completos
order by 1, 2
```

**11 y 12 · Barras por fase** (una serie por fase para fijar el color). 11 usa `avg(anomalia_temperatura_c)`; 12 usa
`100 * (sum(lluvia_total_mm) / sum(lluvia_referencia_mm) - 1)`:
```sql
select fase_enso as "Fase",
       case when fase_enso = 'La Niña' then round(avg(anomalia_temperatura_c)::numeric, 2) end as "La Niña",
       case when fase_enso = 'Neutral' then round(avg(anomalia_temperatura_c)::numeric, 2) end as "Neutral",
       case when fase_enso = 'El Niño' then round(avg(anomalia_temperatura_c)::numeric, 2) end as "El Niño"
from grupo06.fct_clima_mensual
where mes_completo and fase_enso <> 'Sin clasificar'
group by fase_enso
order by case fase_enso when 'La Niña' then 1 when 'Neutral' then 2 else 3 end
```

**13 y 14 · Barras por mes, coloreadas por fase.** 13 usa `anomalia_temperatura_c`; 14 usa
`round((lluvia_total_mm - lluvia_referencia_mm)::numeric, 0)`:
```sql
select mes as "Mes",
       case when fase_enso = 'La Niña' then anomalia_temperatura_c end as "La Niña",
       case when fase_enso = 'Neutral' then anomalia_temperatura_c end as "Neutral",
       case when fase_enso = 'El Niño' then anomalia_temperatura_c end as "El Niño"
from grupo06.fct_clima_mensual
where mes_completo
order by mes
```

## Por qué carga rápido
Todas las consultas leen tablas **ya calculadas por dbt**: ~60 filas mensuales y ~1.800 diarias, con los features
(amplitud, días desde la última lluvia, desvío vs. media de 30 días, lluvia del día) ya resueltos. Metabase solo agrupa
pocas filas; no recalcula nada que deba hacer el pipeline.

## Metabase y Grafana: cada uno en su rol
Metabase responde preguntas **sobre el clima** (negocio). La salud del **pipeline** (si corrió, cuánto tardó, si el dato
está al día) es observabilidad y va en Grafana: ver [grafana/README.md](grafana/README.md).
