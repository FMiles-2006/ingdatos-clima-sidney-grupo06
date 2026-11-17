-- Cada mes tiene que caer en UNA sola fase ENSO: los períodos del seed no se pisan.
-- Si alguien agrega una fase nueva sin cerrar la anterior, este test falla.
select a.desde, a.fase, b.desde as desde_otro, b.fase as fase_otra
from {{ ref('stg_fases_enso') }} a
join {{ ref('stg_fases_enso') }} b
  on a.desde < b.desde
 and coalesce(a.hasta, date '9999-12-01') >= b.desde
