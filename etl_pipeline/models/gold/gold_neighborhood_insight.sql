{{
    config(
        materialized='table',
        tags=['gold', 'neighborhood', 'dashboard']
    )
}}

with neighborhood_metrics as (
    select
        coalesce(neighborhood, 'Unknown') as neighborhood,
        coalesce(supervisor_district, 0) as supervisor_district,
        
        -- Volume metrics
        count(*) as total_cases,
        count(case when status = 'Closed' then 1 end) as closed_cases,
        count(case when status = 'Open' then 1 end) as open_cases,
        
        -- Performance metrics
        round(avg(case when status = 'Closed' then response_time_hours end), 1) as avg_response_time_hours,
        count(case when status = 'Closed' and response_time_hours <= 24 then 1 end) as cases_closed_within_24h,
        
        -- Service type analysis
        mode() within group (order by service_name) as most_common_service,
        count(distinct service_name) as unique_service_types,
        
        -- Top services (array for dashboard drill-down)
        array_agg(distinct service_name) as all_service_types,
        
        -- Geographic density (simplified)
        count(*) / 1.0 as case_density,  -- Would normalize by area/population if available
        
        -- Timeliness
        count(case when status = 'Open' and opened < current_date - interval '7 days' then 1 end) as aging_cases
        
    from {{ ref('silver_sf_311') }}
    where opened >= current_date - interval '12 months'
    group by 1, 2
),

ranked_neighborhoods as (
    select
        *,
        row_number() over (order by total_cases desc) as volume_rank,
        row_number() over (order by avg_response_time_hours desc nulls last) as response_time_rank,
        case_density * 1.0 as priority_score  -- Simplified priority calculation
    from neighborhood_metrics
    where total_cases
)

select
    neighborhood,
    supervisor_district,
    total_cases,
    closed_cases,
    open_cases,
    
    -- Performance metrics
    round(closed_cases::decimal / total_cases * 100, 1) as completion_rate,
    avg_response_time_hours,
    round(cases_closed_within_24h::decimal / nullif(closed_cases, 0) * 100, 1) as sla_24h_rate,
    
    -- Service analysis
    most_common_service,
    unique_service_types,
    all_service_types,
    
    -- Priority indicators
    volume_rank,
    response_time_rank,
    priority_score,
    aging_cases,
    round(aging_cases::decimal / nullif(open_cases, 0) * 100, 1) as aging_rate,
    
    -- Alert flags for dashboard
    case 
        when avg_response_time_hours > 72 then 'High'
        when avg_response_time_hours > 48 then 'Medium'
        else 'Low'
    end as response_time_alert,
    
    case
        when aging_rate > 50 then 'Critical'
        when aging_rate > 25 then 'High'
        else 'Normal'
    end as backlog_alert

from ranked_neighborhoods
order by priority_score desc