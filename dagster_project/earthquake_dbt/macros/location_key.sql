{# Surrogate key shared by dim_location and fact_earthquake. #}
{% macro location_key(region_expr) %}
    cityHash64({{ region_expr }})
{% endmacro %}
