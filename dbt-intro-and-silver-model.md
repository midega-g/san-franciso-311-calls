# Transforming Raw Data with dbt: Building a Silver Layer for Analytics

## Introduction to dbt in the Modern Data Stack

Data Build Tool (dbt) represents a paradigm shift in how data teams approach transformation workflows within modern data warehouses. Unlike traditional ETL tools that require complex infrastructure and specialized knowledge, dbt operates as a command-line tool that transforms data using familiar SQL syntax while providing software engineering best practices like version control, testing, and documentation. This approach enables analytics engineers to build reliable, maintainable data pipelines that bridge the gap between raw data ingestion and business intelligence consumption.

The integration of dbt into our San Francisco 311 calls pipeline demonstrates how transformation logic can be separated from extraction and loading concerns, creating a more modular and testable architecture. By working directly within the data warehouse, dbt eliminates the need for external processing engines while leveraging the computational power of modern analytical databases like PostgreSQL with PostGIS extensions.

## Environment Setup and dbt Installation

The dbt installation process integrates seamlessly with the existing `uv` package management workflow established in the earlier pipeline components. The installation requires both the core dbt functionality and the PostgreSQL adapter to enable communication with the target database.

```bash
uv add dbt-core dbt-postgres
```

This installation approach maintains consistency with the project's dependency management strategy while ensuring that dbt operates within the same virtual environment as other pipeline components. The `dbt-postgres` adapter specifically enables dbt to generate PostgreSQL-compatible SQL and handle database-specific features like PostGIS spatial functions.

Project initialization follows dbt's standard workflow, which creates the necessary directory structure and configuration files through an interactive setup process.

```bash
uv run dbt init
```

The initialization process prompts for several configuration parameters that define how dbt connects to and interacts with the target database. The project name `etl_pipeline` establishes the root namespace for all dbt artifacts, while the database selection of `postgres` determines which SQL dialect and adapter dbt will use for code generation.

Connection parameters require careful consideration of the existing Docker infrastructure. The hostname `localhost` reflects the fact that dbt runs outside the Docker network and connects to the PostgreSQL container through the exposed port mapping. The port `5400` corresponds to the host-side port mapping defined in the Docker Compose configuration, while the internal container port remains `5432`. The database name `sf_311_calls_db` matches the PostgreSQL database created during container initialization, ensuring dbt targets the correct data repository.

The schema parameter `sf_311_dbt` creates a dedicated namespace within the database for dbt-managed objects, preventing conflicts with manually created tables and providing clear separation between raw data and transformed models. The thread count of `5` enables parallel execution of dbt operations, improving performance when processing multiple models simultaneously while remaining conservative enough to avoid overwhelming the database connection pool.

## Profile Configuration and Database Connectivity

The dbt profile configuration resides in the user's home directory at `~/.dbt/profiles.yml`, establishing a centralized location for database connection parameters that can be shared across multiple dbt projects. This separation of connection details from project code enables the same dbt models to be deployed across different environments without code modifications.

```bash
cat ~/.dbt/profiles.yml
sudo nano ~/.dbt/profiles.yml
```

The profile name change from `etl_pipeline` to `sf_311_etl_pipeline` in both `profiles.yml` and `dbt_project.yml` reflects a best practice of using descriptive, unique profile names that clearly identify the target system and use case. This naming convention becomes particularly important in environments where multiple dbt projects might target different databases or schemas, preventing accidental cross-project interference.

```yaml
# profiles.yml file
sf_311_etl_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5400
      user: root
      password: root
      dbname: sf_311_calls_db
      schema: sf_311_dbt
      threads: 5
```

The profile structure defines multiple output targets, with `dev` representing the development environment configuration. This pattern enables the same dbt project to be deployed across development, staging, and production environments by simply changing the target parameter, while maintaining environment-specific connection details within the profile.

## Project Structure and Configuration Management

The `dbt_project.yml` file serves as the central configuration hub for the entire dbt project, defining how models are organized, materialized, and deployed within the target database. This file establishes the project's identity and behavior patterns that apply across all contained models.

```yaml
# dbt_project.yml file
name: 'etl_pipeline'
version: '1.0.0'
profile: 'sf_311_etl_pipeline'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

models:
  etl_pipeline:
    silver:
      +schema: silver  
    gold:
      +schema: gold
```

The directory structure reflects dbt's opinionated approach to organizing analytical code, with each directory type serving a specific purpose in the transformation workflow. The `models` directory contains the core transformation logic, while `macros` houses reusable SQL functions, `tests` defines data quality validations, and `seeds` manages small reference datasets that can be version-controlled alongside the code.

The removal of the default `examples` folder and creation of `silver` and `gold` directories implements the medallion architecture pattern within dbt's organizational framework. This structure separates cleaned, typed data (silver) from business-ready, aggregated datasets (gold), creating clear boundaries between different levels of data refinement and enabling teams to understand data lineage at a glance.

```bash
rm -rf models/example
mkdir -p models/silver models/gold
```

The schema configuration using `+schema: silver` and `+schema: gold` instructs dbt to create separate database schemas for each layer, resulting in schema names like `sf_311_dbt_silver` and `sf_311_dbt_gold`. This naming convention combines the base schema from the profile configuration with the layer-specific suffix, creating logical separation while maintaining clear relationships between different data layers.

## Package Management and Third-Party Dependencies

Modern dbt development relies heavily on community-maintained packages that provide pre-built macros and utilities for common data transformation tasks. The `packages.yml` file defines these dependencies in a declarative format that enables reproducible builds across different environments.

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.3.1
```

The `dbt_utils` package represents the most widely adopted utility library in the dbt ecosystem, providing macros for data quality testing, SQL generation, and common transformation patterns. Version pinning ensures consistent behavior across deployments while preventing unexpected breaking changes from automatic updates.

```bash
dbt deps
```

The `dbt deps` command downloads and installs the specified packages into the `dbt_packages` directory, making their macros and models available for use within the project. This directory should be excluded from version control since it contains generated content that can be recreated from the package specification.

## Source Definition and Data Lineage

The source configuration in `sf_311_sources.yml` establishes the connection between dbt models and the raw data tables created by the earlier ETL pipeline components. This configuration serves multiple purposes: it documents the expected structure of upstream data, enables dbt to generate proper SQL references, and provides a foundation for data quality testing at the source level.

```yaml
version: 2

sources:
  - name: bronze_source
    database: sf_311_calls_db
    schema: bronze
    tables:
      - name: sf_311_calls
        columns:
          - name: service_request_id
            tests:
              - unique
              - not_null
          - name: requested_datetime
            tests:
              - not_null
```

The source definition creates a logical reference that can be used in dbt models through the `source()` function, generating SQL that references the actual table location while providing an abstraction layer that simplifies model code. The database and schema parameters must match the actual location of the raw data tables, ensuring dbt can generate correct SQL references.

Source-level testing provides an early warning system for data quality issues, validating assumptions about the raw data before transformation begins. The `unique` and `not_null` tests on `service_request_id` ensure that the primary key constraints expected by downstream models are satisfied, while the `not_null` test on `requested_datetime` validates that essential timestamp information is present.

## Silver Model Implementation and Data Transformation

The silver layer model represents the core transformation logic that converts raw, untyped data into a clean, business-ready format suitable for analytical consumption. The `silver_sf_311.sql` model demonstrates advanced dbt features including incremental processing, data type conversion, spatial data handling, and derived column creation.

```sql
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
        -- ... additional transformations
        case
            when closed_date is not null then 
                extract(epoch from (closed_date::TIMESTAMP - requested_datetime::TIMESTAMP)) / 3600
            else null
        end as response_time_hours
    from {{ source('bronze_source', 'sf_311_calls') }}
    {% if is_incremental() %}
        where cast(data_as_of as TIMESTAMP) > (select coalesce(max(cast(data_as_of as TIMESTAMP)), '1900-01-01'::TIMESTAMP) from {{ this }})
           or cast(service_request_id as BIGINT) not in (select case_id from {{ this }})
    {% endif %}
)

select * from cleaned
```

The configuration block at the model's beginning defines critical behavior patterns that determine how dbt materializes and maintains the resulting table. The `materialized='incremental'` setting enables efficient processing of large datasets by only processing new or changed records rather than rebuilding the entire table on each run. This approach becomes essential when dealing with large datasets where full rebuilds would be time-prohibitive.

The `unique_key='case_id'` parameter identifies which column dbt should use to determine record uniqueness during incremental processing. This key must reference the final column name in the SELECT statement rather than the source column name, reflecting the transformation from `service_request_id` to `case_id` that occurs within the model logic.

The `incremental_strategy='delete+insert'` approach provides a balance between performance and data consistency by first deleting existing records that match the unique key, then inserting the new versions. This strategy handles both new records and updates to existing records while maintaining referential integrity.

The `post_hook="ANALYZE {{ this }};"` directive instructs PostgreSQL to update table statistics after the model completes, ensuring that the query planner has accurate information for optimizing future queries against the transformed data. The `{{ this }}` variable resolves to the fully qualified name of the current model's table, enabling the hook to reference the correct target regardless of schema or naming changes.

## Data Type Conversion and Cleaning Logic

The transformation logic within the silver model addresses several data quality challenges inherent in API-sourced data. The original data arrives as TEXT fields due to the bronze layer's schema design, requiring explicit type conversion to enable proper analytical operations and storage optimization.

```sql
cast(service_request_id as BIGINT) as case_id,
requested_datetime::TIMESTAMP as opened,
cast(cast(supervisor_district as DOUBLE PRECISION) as INTEGER) as supervisor_district,
lat::NUMERIC as latitude,
long::NUMERIC as longitude,
```

The double colon syntax (`::`) represents PostgreSQL's shorthand for type casting, providing a more concise alternative to the standard `CAST()` function while maintaining identical functionality. This syntax becomes particularly valuable in complex transformations where multiple type conversions are required.

The conversion of `supervisor_district` demonstrates a common pattern when dealing with numeric data that may contain decimal values but should be stored as integers. The intermediate conversion to `DOUBLE PRECISION` handles cases where the source data contains decimal representations of whole numbers, while the final conversion to `INTEGER` ensures proper storage and indexing behavior.

String cleaning operations using the `TRIM()` function remove leading and trailing whitespace that commonly appears in API responses, ensuring consistent data quality and preventing issues with string matching operations in downstream analyses.

## Spatial Data Processing and PostGIS Integration

The handling of geographic data represents one of the most complex aspects of the silver model transformation, requiring conversion from text-based GeoJSON representations to native PostGIS geometry objects that support spatial operations and indexing.

```sql
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
```

The spatial data transformation addresses a common issue with API-sourced GeoJSON data where single quotes are used instead of the double quotes required by the JSON specification. The `regexp_replace()` function performs a global substitution to correct this formatting issue before passing the data to PostGIS functions.

The `ST_GeomFromGeoJSON()` function converts the corrected JSON representation into a native PostGIS geometry object, enabling spatial operations like distance calculations, containment queries, and spatial joins with other geographic datasets. This conversion is essential for leveraging PostgreSQL's spatial indexing capabilities and the full range of PostGIS analytical functions.

The Docker infrastructure requires updating to the `postgis/postgis:14-3.5` image to provide the necessary spatial extensions. This change from the standard PostgreSQL image adds comprehensive spatial data support while maintaining compatibility with existing relational operations.

## Incremental Processing Strategy

The incremental processing logic implements a sophisticated approach to handling both new records and updates to existing data, using multiple criteria to determine which records require processing during each dbt run. However, careful attention must be paid to column name transformations that occur within the model to ensure the incremental logic references the correct fields.

```sql
{% if is_incremental() %}
    where cast(data_as_of as TIMESTAMP) > (
        select coalesce(max(cast(data_as_of as TIMESTAMP)), '1900-01-01'::TIMESTAMP) 
        from {{ this }}
    )
    or cast(service_request_id as BIGINT) not in (select case_id from {{ this }})
{% endif %}
```

The `is_incremental()` macro determines whether the current run should process only new data or perform a full rebuild. On the initial run, this condition evaluates to false, causing dbt to process all available source data. On subsequent runs, the condition becomes true, activating the incremental logic.

The `data_as_of` timestamp comparison identifies records that have been updated in the source system since the last dbt run, ensuring that changes to existing cases are captured and reflected in the silver layer. The `COALESCE()` function handles the edge case where no previous data exists by providing a default timestamp that ensures all records are processed.

The critical aspect of the second condition involves the column name transformation that occurs within the model. The source table contains `service_request_id` as a TEXT field, while the silver model transforms this to `case_id` as a BIGINT. The incremental logic must account for this transformation by casting the source `service_request_id` to BIGINT and comparing it against the `case_id` column in the existing silver table. This ensures that the comparison operates on compatible data types and references the correct columns in each table.

This column name mismatch represents a common pitfall in incremental dbt models where transformations change field names or types. The incremental logic must always reference the final column names and types as they exist in the target table, while properly casting or transforming the source columns to match. Failure to account for these transformations can result in incomplete data processing, where only a subset of available records are processed during incremental runs, leading to data gaps that may not be immediately apparent but can significantly impact downstream analytics and reporting.

## Derived Column Creation and Business Logic

The silver model introduces calculated fields that provide immediate analytical value while maintaining the raw data for audit and reprocessing purposes. The `response_time_hours` calculation demonstrates how business logic can be embedded within the transformation layer to support common analytical use cases.

```sql
case
    when closed_date is not null then 
        extract(epoch from (closed_date::TIMESTAMP - requested_datetime::TIMESTAMP)) / 3600
    else null
end as response_time_hours
```

This calculation converts the time difference between case opening and closure into hours, providing a standardized metric for measuring service efficiency across different case types and agencies. The `EXTRACT(epoch FROM ...)` function returns the difference in seconds, which is then divided by 3600 to convert to hours.

The conditional logic ensures that response time is only calculated for closed cases, returning NULL for open cases where the closure time is not yet available. This approach prevents misleading metrics while maintaining data integrity for ongoing cases.

The inclusion of derived columns in the silver layer reflects a design philosophy that balances raw data preservation with analytical convenience. By calculating common metrics at the transformation stage, downstream consumers can focus on business analysis rather than repetitive calculation logic.

## Data Quality Testing and Validation Framework

The silver model implements a comprehensive testing strategy that combines built-in dbt tests with custom validation logic tailored to the specific characteristics of 311 service data. The testing framework operates at multiple levels, validating both individual column constraints and cross-column business rules.

```yaml
columns:
  - name: case_id
    tests:
      - unique
      - not_null
  - name: latitude
    tests:
      - valid_latitude
  - name: longitude
    tests:
      - valid_longitude
  - name: point_geom
    tests:
      - valid_geometry
  - name: response_time_hours
    tests:
      - valid_response_time
```

Standard dbt tests like `unique` and `not_null` provide fundamental data integrity validation, ensuring that primary keys maintain their constraints and essential fields contain valid data. These tests run efficiently as simple SQL queries that return failing records, making them suitable for large datasets and frequent execution.

Custom tests implemented as macros address domain-specific validation requirements that cannot be expressed through generic test patterns. The geographic validation tests demonstrate this approach by implementing business rules specific to San Francisco's geographic boundaries.

```sql
{% test valid_latitude(model, column_name) %}
    SELECT 
        case_id
    FROM {{ model }}
    WHERE {{ column_name }} NOT BETWEEN 37.73 AND 37.83
      AND {{ column_name }} IS NOT NULL
{% endtest %}
```

The custom test macro structure follows dbt's testing conventions while enabling complex validation logic that would be difficult to express through configuration alone. The test returns records that fail the validation criteria, allowing dbt to report specific failures and enabling data teams to investigate and resolve quality issues.

The choice to implement tests as macros rather than placing them in the `tests` directory reflects the reusable nature of these validations. Macros can be applied to multiple models and columns, while tests in the `tests` directory are typically model-specific and less reusable across the project.

The spatial geometry validation using `ST_IsValid()` demonstrates integration with PostGIS functions within the testing framework, ensuring that spatial transformations produce valid geometric objects that can be used reliably in downstream spatial analyses.

```sql
{% test valid_geometry(model, column_name) %}
    SELECT 
        case_id
    FROM {{ model }}
    WHERE NOT ST_IsValid({{ column_name }})
      AND {{ column_name }} IS NOT NULL
{% endtest %}
```

## Documentation Strategy and Knowledge Management

The documentation approach implemented through the `docs.md` file and `doc()` function calls creates a centralized knowledge repository that travels with the code, ensuring that business context and technical details remain accessible to future developers and analysts.

```markdown
{% docs case_id %}
Unique identifier for each 311 service request. This is the primary key that tracks individual service cases from request to completion. Originally named `service_request_id` in the raw data.
{% enddocs %}
```

The documentation blocks provide rich context about each field's business meaning, data lineage, and usage patterns. This information becomes particularly valuable in complex domains like municipal services where field names may not be self-explanatory and business rules may not be obvious to new team members.

The `{{ doc('case_id') }}` function calls in the schema definition create dynamic links between the model specification and the documentation content, ensuring that documentation remains visible in dbt's generated documentation site and stays synchronized with model changes.

This documentation strategy addresses a common challenge in analytical projects where business knowledge becomes siloed or lost over time. By embedding documentation within the code repository and making it part of the standard development workflow, teams can maintain institutional knowledge and reduce onboarding time for new contributors.

## Command-Line Operations and Development Workflow

The dbt command-line interface provides a rich set of operations for developing, testing, and deploying transformation logic. The development workflow typically involves iterative cycles of model development, compilation, testing, and documentation generation.

```bash
dbt compile
dbt run --select silver_sf_311
dbt test --select silver_sf_311
dbt docs generate
dbt docs serve --port 8081
```

The `dbt compile` command validates SQL syntax and resolves all Jinja templating without executing the queries against the database. This operation provides rapid feedback during development and enables developers to inspect the generated SQL before committing to database execution.

Selective execution using `--select` enables focused development and testing of individual models without processing the entire project. This capability becomes essential in large projects where full rebuilds would be time-prohibitive during iterative development cycles.

The documentation server using `--port 8081` avoids conflicts with other services that might be using the default port, demonstrating the importance of port management in development environments with multiple running services.

The `target` directory contains all generated artifacts from dbt operations, including compiled SQL, run results, and documentation assets. This directory should be excluded from version control since it contains generated content that can be recreated from the source code, but it provides valuable debugging information during development.

## Schema Generation and Database Object Management

The execution of `dbt run` results in the creation of database schemas that follow the naming convention established in the project configuration. The schema name `sf_311_dbt_silver` combines the base schema from the profile (`sf_311_dbt`) with the layer-specific suffix (`silver`), creating logical separation between different data layers while maintaining clear relationships.

This naming approach enables multiple dbt projects to coexist within the same database without conflicts while providing intuitive organization that reflects the medallion architecture pattern. Database administrators can easily identify dbt-managed objects and understand their role within the broader data architecture.

The incremental materialization strategy creates persistent tables that accumulate data over time, requiring careful consideration of storage management and maintenance procedures. The `ANALYZE` post-hook ensures that PostgreSQL maintains accurate statistics for query optimization, but additional maintenance tasks like `VACUUM` may be required for optimal performance in production environments.

This comprehensive approach to data transformation demonstrates how dbt enables analytics teams to implement sophisticated data processing logic while maintaining the reliability, testability, and documentation standards expected in modern software development practices.
