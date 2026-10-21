-- Test de los features: recalcula el promedio móvil de 7 días usando SOLO datos con
-- fecha <= la del feature. Si el modelo de features mirara el futuro, no coincidiría.
with recalculo as (
    select f.fecha,
           f.temp_media_movil_7d_c,
           round(avg(s.temp_media_c), 2) as esperado
    from {{ ref('feat_clima_diario') }} f
    join {{ ref('stg_clima_diario') }} s
      on s.fecha between f.fecha - interval 6 days and f.fecha
    group by 1, 2
)
select * from recalculo
where abs(temp_media_movil_7d_c - esperado) > 0.01
