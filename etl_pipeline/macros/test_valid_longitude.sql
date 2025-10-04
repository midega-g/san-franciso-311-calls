{% test valid_longitude(model, column_name) %}
    SELECT 
        case_id
    FROM {{ model }}
    WHERE {{ column_name }} NOT BETWEEN -122.52 AND -122.36
      AND {{ column_name }} IS NOT NULL
{% endtest %}