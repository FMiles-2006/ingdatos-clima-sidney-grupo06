-- Test propio del dominio: la serie diaria es continua. Entre la primera y la última
-- fecha no puede faltar ningún día. Detecta un watermark que avanzó salteando días
-- (p. ej. una corrida que escribió un rango parcial) o un filtro de silver demasiado agresivo.
with limites as (
    select min(fecha) as desde, max(fecha) as hasta, count(*) as dias
    from {{ ref('stg_clima_diario') }}
)
select desde, hasta, dias, (hasta - desde) + 1 as dias_esperados
from limites
where dias <> (hasta - desde) + 1
