-- SILVER · un hecho por día: el clima observado en Sídney ese día local.
-- Lee de la tabla Delta de bronze (source 'bronze.clima_diario'), NO de un seed.
--
-- Qué limpia (ver README_E2):
--   1. Tipos: fecha texto -> DATE; códigos y grados -> INTEGER; medidas -> DOUBLE redondeado.
--   2. Duplicados: si una fecha aparece más de una vez (reproceso, carga manual) se queda
--      la última ingerida. Bronze ya hace MERGE; esto es defensa en profundidad.
--   3. rain_sum se descarta: no significa lo mismo en las dos fuentes (en la Historical
--      Forecast API excluye chaparrones). La lluvia del día es precipitation_sum.
--   4. Dirección del viento: 360° y 0° son el mismo norte -> se normaliza a [0, 360).
--   5. Días sin ninguna medición (no publicados aún) se excluyen.
--   6. Se agregan atributos derivados del propio día: estación austral, día lluvioso
--      (>= 1 mm, criterio del Bureau of Meteorology) y amplitud térmica.

with bronze as (
    select * from {{ source('bronze', 'clima_diario') }}
),

deduplicado as (
    select *,
           row_number() over (partition by fecha order by ingestado_en desc) as _orden
    from bronze
    where temperature_2m_mean is not null
       or precipitation_sum   is not null
),

tipado as (
    select
        cast(fecha as date)                                   as fecha,
        cast(weather_code as integer)                         as codigo_wmo,
        round(cast(temperature_2m_max as double), 1)          as temp_max_c,
        round(cast(temperature_2m_min as double), 1)          as temp_min_c,
        round(cast(temperature_2m_mean as double), 1)         as temp_media_c,
        round(cast(apparent_temperature_mean as double), 1)   as sensacion_termica_media_c,
        round(cast(precipitation_sum as double), 1)           as precipitacion_mm,
        round(cast(precipitation_hours as double), 0)         as horas_con_precipitacion,
        round(cast(wind_speed_10m_max as double), 1)          as viento_max_kmh,
        round(cast(wind_gusts_10m_max as double), 1)          as rafaga_max_kmh,
        cast(round(cast(wind_direction_10m_dominant as double)) as integer) % 360
                                                              as direccion_viento_grados,
        round(cast(shortwave_radiation_sum as double), 2)     as radiacion_solar_mj_m2,
        fuente,
        cast(ingestado_en as timestamp)                       as ingestado_en
    from deduplicado
    where _orden = 1
)

select
    fecha,
    case
        when month(fecha) in (12, 1, 2) then 'Verano'
        when month(fecha) in (3, 4, 5)  then 'Otoño'
        when month(fecha) in (6, 7, 8)  then 'Invierno'
        else 'Primavera'
    end                                          as estacion,
    codigo_wmo,
    temp_max_c,
    temp_min_c,
    temp_media_c,
    sensacion_termica_media_c,
    round(temp_max_c - temp_min_c, 1)            as amplitud_termica_c,
    precipitacion_mm,
    precipitacion_mm >= 1.0                      as es_dia_lluvioso,
    horas_con_precipitacion,
    viento_max_kmh,
    rafaga_max_kmh,
    direccion_viento_grados,
    radiacion_solar_mj_m2,
    fuente,
    fuente = 'archive_era5'                      as es_reanalisis,
    ingestado_en
from tipado
