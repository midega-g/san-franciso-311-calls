{{
    config(
        materialized='table',
        tags=['gold', 'agency', 'dashboard']
    )
}}

with agency_stats as (
    select
        responsible_agency,
        count(*) as total_cases,
        count(case when status = 'Closed' then 1 end) as closed_cases,
        count(case when status = 'Open' then 1 end) as open_cases,
        round(avg(case when status = 'Closed' then response_time_hours end), 1) as avg_response_time_hours,
        count(case when status = 'Closed' and response_time_hours <= 24 then 1 end) as cases_closed_within_24h,
        count(case when status = 'Closed' and response_time_hours <= 72 then 1 end) as cases_closed_within_72h,
        
        -- Backlog analysis
        count(case when status = 'Open' and opened < current_date - interval '7 days' then 1 end) as cases_open_over_7_days,
        count(case when status = 'Open' and opened < current_date - interval '30 days' then 1 end) as cases_open_over_30_days,
        
        -- Current month vs previous month comparison
        count(case when status = 'Closed' and date_trunc('month', closed) = date_trunc('month', current_date) 
              then 1 end) as closed_current_month,
        count(case when status = 'Closed' and date_trunc('month', closed) = date_trunc('month', current_date - interval '1 month') 
              then 1 end) as closed_previous_month
    from {{ ref('silver_sf_311') }}
    where opened >= current_date - interval '6 months'
    group by 1
)

select
    responsible_agency,
    total_cases,
    closed_cases,
    open_cases,
    
    -- Performance metrics
    round(closed_cases::decimal / total_cases * 100, 1) as completion_rate,
    avg_response_time_hours,
    round(cases_closed_within_24h::decimal / nullif(closed_cases, 0) * 100, 1) as sla_24h_rate,
    round(cases_closed_within_72h::decimal / nullif(closed_cases, 0) * 100, 1) as sla_72h_rate,
    
    -- Backlog metrics
    cases_open_over_7_days,
    cases_open_over_30_days,
    round(cases_open_over_7_days::decimal / nullif(open_cases, 0) * 100, 1) as backlog_7d_rate,
    
    -- Trend analysis
    case 
        when closed_previous_month = 0 then 0
        else round((closed_current_month - closed_previous_month)::decimal / closed_previous_month * 100, 1)
    end as monthly_completion_change_pct
    
    
from agency_stats
where total_cases
order by total_cases desc