# Building Business-Ready Analytics: Gold Layer Implementation for Decision Making

## Introduction to Gold Layer Analytics

The gold layer represents the final stage of the medallion architecture, where cleaned and typed data from the silver layer is transformed into business-ready analytics that directly answer stakeholder questions. Unlike the silver layer which focuses on data quality and standardization, the gold layer prioritizes business value and decision-making support through pre-calculated metrics, aggregations, and insights that align with organizational reporting needs.

The San Francisco 311 calls gold layer implementation demonstrates how analytical models can be designed around specific business questions rather than technical data structures. Each gold model serves as a purpose-built data mart that addresses distinct stakeholder needs, from executive dashboards requiring high-level KPIs to operational teams needing detailed performance metrics for day-to-day management decisions.

## Business Question-Driven Model Design

The gold layer architecture organizes models around the fundamental questions that different stakeholders need answered, creating a clear mapping between business requirements and data products. This approach ensures that analytical outputs directly support decision-making processes rather than requiring additional interpretation or calculation by end users.

The executive summary model addresses leadership questions about overall system performance and trends. Questions like "How many cases did we handle this month?" and "What's our average response time?" require aggregated metrics that provide organizational oversight without overwhelming detail. The model focuses on high-level KPIs that enable executives to assess whether the 311 system is meeting citizen service expectations and identify periods requiring deeper investigation.

```sql
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
    group by 1
)
```

The agency performance model targets operational managers who need to understand departmental efficiency and resource allocation. Questions such as "Which departments are fastest/slowest?" and "Who has the biggest backlog?" require comparative analysis across agencies with metrics that highlight both performance achievements and areas needing attention. This model enables data-driven decisions about resource allocation, process improvements, and inter-agency coordination.

```sql
with agency_stats as (
    select
        responsible_agency,
        count(*) as total_cases,
        count(case when status = 'Closed' then 1 end) as closed_cases,
        count(case when status = 'Open' then 1 end) as open_cases,
        round(avg(case when status = 'Closed' then response_time_hours end), 1) as avg_response_time_hours,
        count(case when status = 'Closed' and response_time_hours <= 24 then 1 end) as cases_closed_within_24h,
        count(case when status = 'Open' and opened < current_date - interval '7 days' then 1 end) as cases_open_over_7_days
    from {{ ref('silver_sf_311') }}
    where opened >= current_date - interval '6 months'
    group by 1
)
```

The neighborhood insights model serves community relations teams and city planners who need geographic analysis of service patterns. Questions like "Which areas have the most service requests?" and "What are the top issues in each neighborhood?" require spatial aggregation and service type analysis that reveals community-specific needs and helps prioritize infrastructure investments or service improvements.

## Materialization Strategy and Performance Optimization

The gold layer models use table materialization rather than views, reflecting the different performance requirements and usage patterns compared to silver layer models. This materialization choice addresses several critical factors that distinguish business analytics from data transformation workflows.

```yaml
{{
    config(
        materialized='table',
        tags=['gold', 'executive', 'dashboard']
    )
}}
```

Table materialization provides consistent query performance for dashboard and reporting applications that require predictable response times regardless of underlying data volume. Unlike views which execute the underlying query logic each time they're accessed, materialized tables store pre-calculated results that can be indexed and optimized for rapid retrieval. This approach becomes essential when gold models contain complex aggregations, window functions, or multi-table joins that would be computationally expensive to execute repeatedly.

The decision to override the default view materialization specified in `dbt_project.yml` reflects the different purposes served by silver and gold layers. Silver models often benefit from view materialization because they primarily serve as transformation steps in larger analytical workflows, where the most current data is always required. Gold models, conversely, serve end-user applications where performance and stability take precedence over real-time data freshness.

The tagging strategy using `['gold', 'executive', 'dashboard']` enables selective execution and deployment of related models, allowing teams to refresh executive dashboards independently from operational reports or neighborhood analysis. This granular control becomes important in production environments where different stakeholder groups have varying requirements for data freshness and update frequency.

## Executive Summary Model Deep Dive

The executive summary model demonstrates sophisticated analytical patterns that transform transactional data into executive-level insights through carefully structured CTEs and business logic. The `monthly_metrics` CTE serves as the foundation for all subsequent calculations, establishing the temporal aggregation framework that enables trend analysis and period-over-period comparisons.

```sql
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
    group by 1
)
```

The `date_trunc('month', opened)` function creates consistent monthly buckets that align with typical executive reporting cycles, ensuring that metrics can be compared across equivalent time periods. This temporal standardization becomes crucial for identifying seasonal patterns, measuring improvement initiatives, and establishing baseline performance expectations.

The conditional aggregation pattern using `count(case when ... then 1 end)` efficiently calculates multiple related metrics in a single pass through the data, avoiding the performance overhead of multiple subqueries or joins. This approach scales well with large datasets while maintaining query readability and logical organization of related calculations.

The service level agreement (SLA) metrics using 24-hour and 72-hour thresholds reflect common municipal service standards that enable benchmarking against other cities and tracking progress toward citizen service commitments. These metrics translate operational performance into business terms that executives can use for public reporting and strategic planning decisions.

The `current_performance` CTE isolates the most recent month's data for dashboard display while maintaining the historical context needed for trend analysis. This pattern enables executive dashboards to highlight current performance while providing drill-down capabilities into historical trends when deeper investigation is required.

## Temporal Data Handling and Date Filtering Strategy

The gold models implement a flexible approach to temporal filtering that balances data freshness with development and testing requirements. The commented date filtering logic demonstrates how analytical models can be adapted for different deployment environments and data availability scenarios.

```sql
where opened >= current_date - interval '12 months'
-- where opened >= '2025-01-01 00:00:00'::TIMESTAMP - interval '12 months'
```

The use of `current_date` provides dynamic filtering that automatically adjusts the analysis window as time progresses, ensuring that reports remain relevant without manual intervention. This approach works well in production environments where the gold models are refreshed regularly and stakeholders expect consistent historical context in their reports.

The hard-coded date alternative serves development and testing scenarios where data availability is limited or where consistent results are needed across multiple development cycles. When working with sample datasets or specific time periods, hard-coded dates ensure that model logic can be validated against known data ranges without the variability introduced by dynamic date calculations.

This dual approach reflects a common pattern in analytical development where models must function correctly across different environments with varying data characteristics. The commented alternatives provide clear documentation of deployment options while maintaining the flexibility needed for both development and production use cases.

## Agency Performance Analytics and Comparative Metrics

The agency performance model implements sophisticated comparative analytics that enable stakeholders to identify high-performing agencies, diagnose operational challenges, and allocate resources based on objective performance data. The model combines volume metrics with efficiency indicators to provide a comprehensive view of agency effectiveness.

```sql
select
    responsible_agency,
    total_cases,
    closed_cases,
    open_cases,
    round(closed_cases::decimal / total_cases * 100, 1) as completion_rate,
    avg_response_time_hours,
    round(cases_closed_within_24h::decimal / nullif(closed_cases, 0) * 100, 1) as sla_24h_rate,
    round(cases_closed_within_72h::decimal / nullif(closed_cases, 0) * 100, 1) as sla_72h_rate,
    cases_open_over_7_days,
    cases_open_over_30_days,
    round(cases_open_over_7_days::decimal / nullif(open_cases, 0) * 100, 1) as backlog_7d_rate
from agency_stats
```

The completion rate calculation provides a fundamental efficiency metric that normalizes performance across agencies with different case volumes, enabling fair comparisons between large departments handling thousands of cases and smaller specialized units. The percentage-based metric facilitates benchmarking and goal-setting while accounting for the natural variation in agency workloads.

The SLA compliance rates using 24-hour and 72-hour thresholds provide actionable performance indicators that align with citizen service expectations and municipal service standards. These metrics enable agencies to track progress toward service commitments while providing city leadership with objective measures of citizen service quality.

The backlog analysis using 7-day and 30-day aging thresholds identifies agencies struggling with case accumulation, enabling proactive intervention before service quality deteriorates. The percentage-based backlog rates normalize the aging analysis across agencies with different case volumes, ensuring that resource allocation decisions are based on relative performance rather than absolute case counts.

The `nullif()` function prevents division-by-zero errors when calculating percentage metrics, ensuring that the model produces valid results even when agencies have no closed cases or open cases in the analysis period. This defensive programming approach maintains model reliability across varying data conditions and agency activity levels.

## Neighborhood Insights and Geographic Analytics

The neighborhood insights model demonstrates advanced analytical techniques for geographic data analysis, combining spatial aggregation with service pattern analysis to reveal community-specific needs and priorities. The model serves multiple stakeholder groups including community relations teams, city planners, and district supervisors who need location-based insights for resource allocation and service planning.

```sql
with neighborhood_metrics as (
    select
        coalesce(neighborhood, 'Unknown') as neighborhood,
        coalesce(supervisor_district, 0) as supervisor_district,
        count(*) as total_cases,
        count(case when status = 'Closed' then 1 end) as closed_cases,
        mode() within group (order by service_name) as most_common_service,
        count(distinct service_name) as unique_service_types,
        array_agg(distinct service_name) as all_service_types,
        count(case when status = 'Open' and opened < current_date - interval '7 days' then 1 end) as aging_cases
    from {{ ref('silver_sf_311') }}
    where opened >= current_date - interval '12 months'
    group by 1, 2
)
```

The `coalesce()` functions handle missing geographic data gracefully by providing default values that prevent records from being excluded from analysis while clearly identifying data quality issues. This approach ensures comprehensive coverage while maintaining data integrity and enabling stakeholders to identify areas where geographic data collection needs improvement.

The `mode() within group (order by service_name)` function identifies the most frequently requested service type in each neighborhood, providing immediate insight into community-specific needs and priorities. This statistical function enables automated identification of neighborhood characteristics without requiring manual analysis of service distributions.

The `array_agg(distinct service_name)` creates a comprehensive list of all service types requested in each neighborhood, enabling dashboard applications to provide drill-down capabilities and detailed service analysis. This approach balances summary-level insights with the detailed information needed for operational planning and community engagement.

The ranking and priority scoring logic provides objective measures for resource allocation and attention prioritization across neighborhoods with varying characteristics and needs.

```sql
ranked_neighborhoods as (
    select
        *,
        row_number() over (order by total_cases desc) as volume_rank,
        row_number() over (order by avg_response_time_hours desc nulls last) as response_time_rank,
        case_density * 1.0 as priority_score
    from neighborhood_metrics
    where total_cases > 0
)
```

The alert classification system provides automated flagging of neighborhoods requiring attention, enabling proactive management and resource allocation based on objective performance thresholds.

```sql
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
```

This comprehensive approach to neighborhood analysis demonstrates how gold layer models can combine multiple analytical techniques to create actionable insights that directly support decision-making processes while maintaining the flexibility needed for various stakeholder requirements and use cases.
