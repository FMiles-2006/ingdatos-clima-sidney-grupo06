-- GOLD · TABLERO. Pregunta: cuantos más días pasan sin llover, ¿más calor y más amplitud?
-- Granularidad: una fila por cantidad de días desde la última lluvia (0 a 6, y "7 o más").
select
    least(dias_desde_ultima_lluvia, 7)                                  as dias_sin_lluvia,
    case when dias_desde_ultima_lluvia >= 7 then '7 o más'
         else cast(dias_desde_ultima_lluvia as varchar) end             as dias_sin_lluvia_etiqueta,
    round(avg(amplitud_termica_c), 1)                                   as amplitud_media_c,
    round(avg(desvio_vs_media_30d_c), 2)                                as temp_vs_media_30d_c,
    count(*)                                                            as dias
from {{ ref('feat_clima_diario') }}
where dias_desde_ultima_lluvia is not null
group by 1, 2
order by 1
