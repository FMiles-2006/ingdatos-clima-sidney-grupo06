-- SILVER · Catálogo de fases de El Niño-Oscilación del Sur (ENSO) según la Oficina de
-- Meteorología de Australia (BoM). Un registro por período continuo con la misma fase.
-- Regla para asignar un mes: la fase vigente el día 15 de ese mes según las declaraciones
-- del BoM (por eso un fin declarado el 21/06 deja junio dentro de la fase anterior).
-- `hasta` nulo = fase vigente hoy.
select
    cast(desde as date)              as desde,
    cast(hasta as date)              as hasta,
    fase,
    referencia
from {{ ref('fases_enso') }}
