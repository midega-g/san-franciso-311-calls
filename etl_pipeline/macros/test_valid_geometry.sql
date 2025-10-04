{% test valid_geometry(model, column_name) %}
    SELECT 
        case_id
    FROM {{ model }}
    WHERE NOT ST_IsValid({{ column_name }})
      AND {{ column_name }} IS NOT NULL
{% endtest %}