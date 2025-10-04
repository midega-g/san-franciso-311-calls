{% test valid_latitude(model, column_name) %}
    SELECT 
        case_id
    FROM {{ model }}
    WHERE {{ column_name }} NOT BETWEEN 37.73 AND 37.83
      AND {{ column_name }} IS NOT NULL
{% endtest %}