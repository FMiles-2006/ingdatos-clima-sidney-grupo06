-- AVISO (no bloquea): el último día cargado tiene más de 3 días de atraso.
-- En una corrida normal el último dato es de ayer. Si el DAG dejó de correr o la API dejó de
-- publicar, ningún otro test lo detecta: la serie sigue sin huecos y los valores son válidos,
-- solo que viejos. Severidad warn: el dato sigue siendo correcto, pero hay que mirar por qué
-- no avanzó (en el CI, con la muestra de bronze de 2025-2026, este aviso aparece siempre).
{{ config(severity='warn') }}
select max(fecha) as ultimo_dia, current_date - max(fecha) as dias_de_atraso
from {{ ref('stg_clima_diario') }}
having current_date - max(fecha) > 3
