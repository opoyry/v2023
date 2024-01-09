{% macro normalize_state(column_name, index) %}
{% set states_dict = {
    "Alabama" : "AL",
    "Alaska" : "AK"
} %}
{% set my_dict = {
                    "HSL": [ "hsl", "7800" ],
                    "Visma Solutions Oy / Maventa":  [ "maventa", "8680" ]
                } %}
case
    {% for k, v in my_dict.items() %}
    when {{ column_name }} = '{{ k }}'
    then '{{ v[index] }}'
    {% endfor %}
end
{% endmacro %}     