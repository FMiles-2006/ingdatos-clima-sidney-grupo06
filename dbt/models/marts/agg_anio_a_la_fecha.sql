-- GOLD · TABLERO. Pregunta: ¿este año viene más cálido o más lluvioso que los anteriores?
-- Compara TODOS los años en el mismo tramo: del 1/1 al día y mes del último dato cargado
-- (así un año en curso no se compara con años completos). Se recalcula en cada corrida.
-- Referencia ("lo normal") = promedio 2022-2025 del mismo tramo.
with ultimo as (
    select max(fecha) as ultima_fecha from {{ ref('feat_clima_diario') }}
),
tramo as (
    select
        year(f.fecha)                                                   as anio,
        avg(f.temp_media_c)                                             as temp_media,
        sum(f.lluvia_mm)                                                as lluvia,
        count(*)                                                        as dias
    from {{ ref('feat_clima_diario') }} f, ultimo u
    where strftime(f.fecha, '%m%d') <= strftime(u.ultima_fecha, '%m%d')
    group by 1
),
referencia as (
    select avg(temp_media) as temp_referencia from tramo where anio between 2022 and 2025
)
select
    t.anio,
    strftime((select ultima_fecha from ultimo), '%d/%m')                as tramo_hasta,
    t.dias,
    round(t.temp_media, 2)                                              as temp_media_tramo_c,
    round(t.temp_media - r.temp_referencia, 2)                          as anomalia_temperatura_c,
    round(t.lluvia, 0)                                                  as lluvia_tramo_mm
from tramo t, referencia r
order by t.anio
