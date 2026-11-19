-- GOLD · TABLERO. Pregunta: en cada estación, ¿cambia la temperatura cuando llueve?
-- Granularidad: una fila por estación (4). Día con lluvia = llovió >= 1 mm ese día.
-- Compara la temperatura media (casi no cambia) y la amplitud térmica (se achica ~3 °C).
with d as (
    select
        fecha,
        case when month(fecha) in (12, 1, 2) then 1 when month(fecha) in (3, 4, 5) then 2
             when month(fecha) in (6, 7, 8)  then 3 else 4 end         as orden_estacion,
        dias_desde_ultima_lluvia = 0                                    as llovio,
        temp_media_c,
        amplitud_termica_c
    from {{ ref('feat_clima_diario') }}
    where dias_desde_ultima_lluvia is not null    -- primeros días de la serie: sin lluvia previa conocida
)
select
    orden_estacion,
    (['Verano', 'Otoño', 'Invierno', 'Primavera'])[orden_estacion]      as estacion,
    round(avg(temp_media_c) filter (where not llovio), 1)               as temp_media_dia_seco_c,
    round(avg(temp_media_c) filter (where llovio), 1)                   as temp_media_dia_con_lluvia_c,
    round(avg(temp_media_c), 1)                                         as temp_media_estacion_c,
    round(avg(amplitud_termica_c) filter (where not llovio), 1)         as amplitud_dia_seco_c,
    round(avg(amplitud_termica_c) filter (where llovio), 1)             as amplitud_dia_con_lluvia_c,
    count(*) filter (where not llovio)                                  as dias_secos,
    count(*) filter (where llovio)                                      as dias_con_lluvia
from d
group by orden_estacion
order by orden_estacion
