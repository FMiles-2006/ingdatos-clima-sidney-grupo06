-- GOLD · Pregunta que responde:
--   "¿Cómo fue el clima de Sídney cada mes (temperatura, lluvia, viento) y cuánto se
--    apartó del promedio de ese mismo mes en el período de referencia 2022-2025?"
-- Granularidad: un registro por mes (silver es un registro por día).

with diario as (
    select d.*, c.categoria as categoria_tiempo
    from {{ ref('stg_clima_diario') }} d
    left join {{ ref('stg_codigos_clima') }} c using (codigo_wmo)
),

mensual as (
    select
        cast(date_trunc('month', fecha) as date)              as mes,
        year(fecha)                                           as anio,
        month(fecha)                                          as mes_numero,
        any_value(estacion)                                   as estacion,
        count(*)                                              as dias_con_datos,
        day(last_day(min(fecha)))                             as dias_del_mes,
        round(avg(temp_media_c), 1)                           as temperatura_media_c,
        round(avg(temp_max_c), 1)                             as maxima_promedio_c,
        round(avg(temp_min_c), 1)                             as minima_promedio_c,
        max(temp_max_c)                                       as maxima_absoluta_c,
        min(temp_min_c)                                       as minima_absoluta_c,
        round(sum(precipitacion_mm), 1)                       as lluvia_total_mm,
        count(*) filter (where es_dia_lluvioso)               as dias_con_lluvia,
        count(*) filter (where temp_max_c >= 30)              as dias_de_calor_30c,
        count(*) filter (where categoria_tiempo = 'Tormenta') as dias_con_tormenta,
        max(rafaga_max_kmh)                                   as rafaga_maxima_kmh,
        round(avg(radiacion_solar_mj_m2), 1)                  as radiacion_solar_media_mj_m2,
        bool_and(es_reanalisis)                               as es_reanalisis
    from diario
    group by 1, 2, 3
),

-- Climatología de referencia: promedio de cada mes calendario en 2022-2025 (ERA5).
referencia as (
    select
        mes_numero,
        avg(temperatura_media_c) as temp_referencia_c,
        avg(lluvia_total_mm)     as lluvia_referencia_mm
    from mensual
    where anio between 2022 and 2025
    group by 1
)

select
    m.mes,
    m.anio,
    m.mes_numero,
    m.estacion,
    m.dias_con_datos,
    m.dias_del_mes,
    m.dias_con_datos = m.dias_del_mes                                   as mes_completo,
    m.temperatura_media_c,
    m.maxima_promedio_c,
    m.minima_promedio_c,
    m.maxima_absoluta_c,
    m.minima_absoluta_c,
    round(m.temperatura_media_c - r.temp_referencia_c, 1)               as anomalia_temperatura_c,
    m.lluvia_total_mm,
    round(r.lluvia_referencia_mm, 1)                                    as lluvia_referencia_mm,
    m.dias_con_lluvia,
    m.dias_de_calor_30c,
    m.dias_con_tormenta,
    m.rafaga_maxima_kmh,
    m.radiacion_solar_media_mj_m2,
    case when m.es_reanalisis then 'ERA5 (reanálisis)' else 'Historical Forecast (modelo)' end
                                                                        as fuente_del_dato
from mensual m
left join referencia r using (mes_numero)
order by m.mes
