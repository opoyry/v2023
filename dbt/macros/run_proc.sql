{% macro run_proc() %}
  {% do run_query("COPY gl_csv TO 'gl_csv.csv' (HEADER, DELIMITER ',')") %}
{% endmacro %}