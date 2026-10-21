-- Test de gold: un mes no puede tener más días con datos que días del calendario,
-- ni más días de lluvia que días con datos. Detecta duplicados que pasen de silver a gold.
select mes, dias_con_datos, dias_del_mes, dias_con_lluvia
from {{ ref('fct_clima_mensual') }}
where dias_con_datos > dias_del_mes
   or dias_con_lluvia > dias_con_datos
