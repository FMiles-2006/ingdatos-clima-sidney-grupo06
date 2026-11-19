-- GOLD · TABLERO. Pregunta: en los meses de La Niña / El Niño, ¿hace más frío o más calor
-- y llueve más o menos que lo habitual para ese mes?
-- Granularidad: una fila por fase. Solo meses completos y clasificados.
select
    case fase_enso when 'La Niña' then 1 when 'Neutral' then 2 else 3 end as orden_fase,
    fase_enso,
    count(*)                                                            as meses,
    round(avg(anomalia_temperatura_c), 2)                               as anomalia_temperatura_c,
    round(100 * (sum(lluvia_total_mm) / sum(lluvia_referencia_mm) - 1), 0) as lluvia_vs_normal_pct
from {{ ref('fct_clima_mensual') }}
where mes_completo and fase_enso <> 'Sin clasificar'
group by fase_enso
order by orden_fase
