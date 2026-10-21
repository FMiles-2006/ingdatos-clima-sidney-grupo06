-- SILVER · catálogo de códigos WMO (estado del tiempo) en español, agrupados en categorías.
-- Es una dimensión de referencia (seed versionado), no datos del pipeline: por eso no
-- viene de bronze. Las dos fuentes usan subconjuntos distintos de códigos; agrupar en
-- 'categoria' es lo que permite comparar períodos.
select
    cast(codigo_wmo as integer) as codigo_wmo,
    descripcion,
    categoria
from {{ ref('codigos_wmo') }}
