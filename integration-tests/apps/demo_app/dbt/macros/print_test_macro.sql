{% macro print_test_macro(message="default", limit=1) %}
    {# 
        A simple macro for testing dbt run-operation.
        It prints arguments to stdout and does not execute SQL.
    #}

    {% do log("print_test_macro called", info=True) %}
    {% do log("message = " ~ message, info=True) %}
    {% do log("limit = " ~ limit, info=True) %}

    {{ return("OK") }}
{% endmacro %}
