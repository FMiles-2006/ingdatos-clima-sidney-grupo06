-- AVISO (no bloquea): hay meses cerrados sin fase de El Niño / La Niña.
-- Significa que el catálogo seeds/fases_enso.csv quedó viejo y hay que actualizarlo con la última
-- declaración de la Oficina de Meteorología de Australia. Severidad warn: el dato del clima sigue
-- siendo válido, solo falta la clasificación.
{{ config(severity='warn') }}
select mes, fase_enso
from {{ ref('fct_clima_mensual') }}
where mes_completo and fase_enso = 'Sin clasificar'
