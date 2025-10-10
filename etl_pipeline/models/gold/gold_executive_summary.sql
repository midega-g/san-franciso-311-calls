{{
    config(
        materialized='table',
        tags=['gold', 'executive', 'dashboard']
    )
}}

with monthly_metrics as (
    select
        date_trunc('month', opened) as month,
        count(*) as total_cases,
        count(case when status = 'Closed' then 1 end) as closed_cases,
        count(case when status = 'Open' then 1 end) as open_cases,
        avg(case when status = 'Closed' then response_time_hours end) as avg_response_time_hours,
        count(case when status = 'Closed' and response_time_hours <= 24 then 1 end) as cases_closed_within_24h,
        count(case when status = 'Closed' and response_time_hours <= 72 then 1 end) as cases_closed_within_72h
    from {{ ref('silver_sf_311') }}
    where opened >= current_date - interval '12 months'
    -- where opened >= '2025-01-01 00:00:00'::TIMESTAMP - interval '12 months'

    group by 1
),

current_performance as (
    select
        total_cases,
        closed_cases,
        open_cases,
        round(closed_cases::decimal / total_cases * 100, 1) as completion_rate,
        round(avg_response_time_hours, 1) as avg_response_time_hours,
        round(cases_closed_within_24h::decimal / closed_cases * 100, 1) as sla_24h_compliance_rate,
        round(cases_closed_within_72h::decimal / closed_cases * 100, 1) as sla_72h_compliance_rate
    from monthly_metrics
    where month = date_trunc('month', current_date)
    -- where month = date_trunc('month', '2025-01-01 00:00:00'::TIMESTAMP)

)

select
    total_cases,
    closed_cases,
    completion_rate,
    avg_response_time_hours,
    sla_24h_compliance_rate,
    sla_72h_compliance_rate
    
from current_performance