{#
  Test genérico propio: el valor debe estar dentro de un rango físicamente posible
  para el clima de Sídney. Falla con las filas fuera de rango (los nulos no cuentan:
  para eso está not_null).
  Uso en schema.yml:
      data_tests:
        - valor_en_rango: {arguments: {minimo: -5, maximo: 50}}
#}
{% test valor_en_rango(model, column_name, minimo, maximo) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < {{ minimo }} or {{ column_name }} > {{ maximo }})
{% endtest %}
