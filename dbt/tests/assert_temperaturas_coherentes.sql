-- Test propio del dominio: en un mismo día, mínima <= media <= máxima.
-- Detecta columnas cruzadas en la API o un error de tipado/redondeo en silver.
select fecha, temp_min_c, temp_media_c, temp_max_c
from {{ ref('stg_clima_diario') }}
where temp_min_c > temp_max_c
   or temp_media_c < temp_min_c - 0.1
   or temp_media_c > temp_max_c + 0.1
