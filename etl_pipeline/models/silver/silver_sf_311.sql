{{ config(
    materialized='incremental',
    unique_key='case_id',
    incremental_strategy='delete+insert',
    post_hook="ANALYZE {{ this }};"
) }}

with cleaned as (
    select
        cast(service_request_id as BIGINT) as case_id,
        requested_datetime::TIMESTAMP as opened,
        closed_date::TIMESTAMP as closed,
        updated_datetime::TIMESTAMP as updated,
        trim(status_description)::TEXT as status,
        trim(status_notes)::TEXT as status_notes,
        trim(agency_responsible)::TEXT as responsible_agency,
        trim(service_name)::TEXT as service_name,
        trim(service_subtype)::TEXT as service_subtype,
        trim(service_details)::TEXT as service_details,
        trim(address)::TEXT as address,
        trim(street)::TEXT as street,
        cast(cast(supervisor_district as DOUBLE PRECISION) as INTEGER) as supervisor_district,
        trim(neighborhoods_sffind_boundaries)::TEXT as neighborhood,
        trim(analysis_neighborhood)::TEXT as analysis_neighborhood,
        trim(police_district)::TEXT as police_district,
        lat::NUMERIC as latitude,
        long::NUMERIC as longitude,
        -- Drop point (redundant)
        -- Parse point_geom to GEOMETRY: Fix single quotes to double for valid GeoJSON
        case 
            when point_geom is not null and point_geom != '' then
                st_geomfromgeojson(
                    regexp_replace(
                        point_geom,
                        '''', '"', 'g'
                    )::JSONB
                ) 
            else null
        end as point_geom,
        trim(source)::TEXT as source_channel,
        media_url::TEXT as media_url,
        cast(cast(bos_2012 as DOUBLE PRECISION) as INTEGER) as supervisor_district_2012,
        data_as_of::TIMESTAMP as data_as_of,
        data_loaded_at::TIMESTAMP as data_loaded_at,
        -- Derived: Basic response time (null if not closed)
        case
            when closed_date is not null then extract(epoch from (closed_date::TIMESTAMP - requested_datetime::TIMESTAMP)) / 3600
            else null
        end as response_time_hours
    from {{ source('bronze_source', 'sf_311_calls') }}
    {% if is_incremental() %}
        where cast(data_as_of as TIMESTAMP) > (select coalesce(max(cast(data_as_of as TIMESTAMP)), '1900-01-01'::TIMESTAMP) from {{ this }})
           or cast(service_request_id as BIGINT) not in (select case_id from {{ this }})
    {% endif %}
)

select * from cleaned