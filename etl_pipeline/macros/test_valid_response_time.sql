{% test valid_response_time(model, column_name) %}
    {#
    Validates that response_time_hours is either:
    - NULL (for open/unresolved cases)
    - >= 0 (for closed cases, indicating positive response time)
    #}
    
    SELECT 
        case_id,
        opened,
        closed,
        {{ column_name }} as response_time_hours,
        'Invalid response time: must be NULL or >= 0' as error_description
    FROM {{ model }}
    WHERE {{ column_name }} < 0
       OR {{ column_name }} = 0 AND closed IS NOT NULL  -- Edge case: closed but 0 hours
{% endtest %}