-- GOLD · FEATURES (feature table): variables derivadas por día, definidas UNA sola vez.
--
-- Semántica: cada fila describe lo que se sabe AL CIERRE del día `fecha`; serviría para
-- predecir el clima del día siguiente. Ningún feature usa datos posteriores a `fecha`:
-- todas las ventanas son "RANGE ... PRECEDING AND CURRENT ROW" sobre la fecha
-- (por calendario, no por cantidad de filas, así un hueco no estira la ventana).
-- El test tests/assert_features_sin_fuga_temporal.sql lo verifica recalculando.

with d as (
    select fecha, temp_media_c, temp_max_c, temp_min_c, precipitacion_mm, es_dia_lluvioso, fuente
    from {{ ref('stg_clima_diario') }}
)

select
    fecha,
    temp_media_c,
    -- Ventanas móviles (solo pasado + el día actual)
    round(avg(temp_media_c) over w7, 2)                     as temp_media_movil_7d_c,
    round(avg(temp_media_c) over w30, 2)                    as temp_media_movil_30d_c,
    round(sum(precipitacion_mm) over w7, 1)                 as lluvia_acumulada_7d_mm,
    round(sum(precipitacion_mm) over w30, 1)                as lluvia_acumulada_30d_mm,
    count(*) over w7                                        as dias_en_ventana_7d,
    -- Variaciones respecto del pasado
    round(temp_media_c - lag(temp_media_c) over (order by fecha), 1)
                                                            as variacion_temp_vs_dia_anterior_c,
    round(temp_max_c - temp_min_c, 1)                       as amplitud_termica_c,
    round(temp_media_c - avg(temp_media_c) over w30, 2)     as desvio_vs_media_30d_c,
    -- Días transcurridos desde la última lluvia (>= 1 mm), incluyendo hoy
    fecha - max(case when es_dia_lluvioso then fecha end)
              over (order by fecha rows between unbounded preceding and current row)
                                                            as dias_desde_ultima_lluvia,
    fuente
from d
window
    w7  as (order by fecha range between interval 6 days  preceding and current row),
    w30 as (order by fecha range between interval 29 days preceding and current row)
