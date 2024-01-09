{% macro run_proc() %}
  {{ log("Exporting transactions to CSV...") }}
  {% do run_query("COPY fct_gl_csv TO 'gl_csv.csv' (HEADER, DELIMITER ',')") %}
  {# {{ debug() }} #}
{% endmacro %}