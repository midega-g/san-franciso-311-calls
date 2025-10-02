# Building a Simple ETL Pipeline: API to Database with Docker

## Project Overview

This project demonstrates the construction of a basic Extract, Transform, Load (ETL) pipeline that pulls San Francisco 311 service call data from a public API and stores it in a PostgreSQL database. The implementation prioritizes simplicity and educational value over production-ready features, making it an ideal starting point for understanding fundamental data engineering concepts.

The pipeline architecture follows a straightforward approach where data extraction, database operations, and infrastructure management are handled through separate, focused modules. This separation allows for clear understanding of each component's role while maintaining the flexibility to enhance individual parts as requirements evolve.

## Environment Setup and Modern Python Tooling

The project leverages `uv` as the package manager, representing a shift toward faster, more reliable Python dependency management. Unlike traditional pip-based workflows, `uv` provides significantly improved resolution times and more predictable virtual environment handling. The choice of Python 3.12 ensures access to the latest language features while maintaining broad compatibility with the selected dependencies.

```bash
uv python install 3.12
uv init san-fransico-311-calls
uv add sodapy duckdb pandas ipykernel psycopg2-binary
```

The dependency selection reflects a minimalist approach focused on core data engineering tasks. The `sodapy` library handles Socrata API interactions, `psycopg2-binary` manages PostgreSQL connectivity, `pandas` provides data manipulation capabilities, and `duckdb` offers analytical query processing. The `ipykernel` dependency enables Jupyter notebook integration for interactive data exploration.

Project initialization follows modern Python packaging standards through the `pyproject.toml` configuration file. This approach ensures reproducible builds and clear dependency declarations, making the project easily shareable and deployable across different environments.

```toml
[project]
name = "san-fransico-311-calls"
version = "0.1.0"
description = "Add your description here"
readme = "README.md"
requires-python = ">=3.12"
dependencies = [
    "duckdb>=1.4.0",
    "ipykernel>=6.30.1",
    "pandas>=2.3.2",
    "psycopg2-binary>=2.9.10",
    "sodapy>=2.2.0",
]
```

## Data Extraction Strategy

The data extraction component implements a deliberately simple approach to API interaction. The `extract_data.py` module creates an unauthenticated Socrata client, which works effectively with San Francisco's open data portal. This design choice eliminates the complexity of credential management while focusing attention on the core data retrieval patterns.

```python
from sodapy import Socrata

client = Socrata("data.sfgov.org", None)

def sample_api_data():
    query = """
    SELECT * 
    WHERE requested_datetime  > "2025-01-01T00:00:00.000"
    LIMIT 10
    """
    results = client.get("vw6y-z8j6", query=query)
    return results
```

The implementation uses SoQL (Socrata Query Language) to perform server-side filtering, requesting only records with timestamps after January 1, 2025. This approach demonstrates an important principle in data engineering: pushing computation to the data source whenever possible. By filtering at the API level rather than retrieving all records and filtering locally, the pipeline reduces network bandwidth usage and minimizes client-side processing requirements.

The LIMIT 10 constraint serves multiple purposes in this educational context. It ensures rapid testing cycles during development, prevents accidental large data downloads that could impact system performance, and provides a manageable dataset size for exploration and debugging. In production scenarios, this would typically be replaced with more sophisticated pagination or batch processing logic.

## Infrastructure Architecture with Docker

The Docker Compose configuration establishes a two-service architecture that demonstrates container orchestration principles while maintaining operational simplicity. The PostgreSQL service uses version 14.17, chosen for its stability and widespread adoption in production environments. The configuration exposes the database on port 5400 to avoid conflicts with locally installed PostgreSQL instances, a common consideration in development environments.

```yaml
services:
  pgdatabase:
    container_name: postgres_container
    image: postgres:14.17
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=sf_311_calls_db 
    volumes:
      - "./data/sf_311_calls_db:/var/lib/postgresql/data:rw"
    ports:
      - "5400:5432"
    networks:
      - pg-network
  pgadmin:
    container_name: pgadmin_container
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
    networks:
      - pg-network
networks:
  pg-network:
    driver: bridge
```

Volume mapping ensures data persistence across container restarts by mounting the local `./data/sf_311_calls_db` directory to the container's data directory. This approach allows developers to examine the raw database files if needed while ensuring that data survives container lifecycle events.

The PgAdmin service provides a web-based interface for database management, accessible at `http://localhost:8080`. This tool proves invaluable for schema inspection, query development, and data verification during the development process. The configuration uses simple credentials (`admin@admin.com`/`root`) appropriate for local development but clearly unsuitable for production deployment.

The bridge network configuration enables seamless communication between services while maintaining isolation from the host network. Services can reference each other using container names rather than IP addresses, simplifying configuration and improving maintainability. When connecting from PgAdmin to PostgreSQL, the hostname becomes `postgres_container` rather than `localhost`, demonstrating how Docker's internal networking operates.

## Database Design and Schema Management

The database schema implementation in `insert_data.py` reveals several important design decisions that balance simplicity with functionality. The choice to store all columns as TEXT data types represents a common pattern in data lake architectures, where preserving original data integrity takes precedence over storage optimization or query performance.

This approach offers several advantages for educational and exploratory purposes. It eliminates type conversion errors during data ingestion, allows for flexible schema evolution as data sources change, and simplifies the initial loading process by avoiding complex data validation logic. The trade-offs include increased storage requirements and the need for explicit type casting in analytical queries, but these concerns are secondary in a learning environment.

The schema organization uses a "bronze" layer pattern, borrowed from medallion architecture concepts. Raw data lands in the bronze schema with minimal transformation, establishing a foundation for future silver and gold layer implementations. This pattern provides clear data lineage and enables reprocessing scenarios where business logic changes require historical data recomputation.

```sql
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.sf_311_calls (
    service_request_id TEXT PRIMARY KEY,
    -- ... other columns as TEXT
    inserted_at TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Africa/Nairobi')
);
```

The `service_request_id` serves as the primary key, ensuring uniqueness while enabling idempotent loading through the `ON CONFLICT DO NOTHING` clause. This design allows the pipeline to be run multiple times without creating duplicate records, an essential characteristic for reliable data processing systems.

## Data Processing Pipeline Implementation

The `insert_data.py` module implements five distinct functions that collectively handle the complete data loading process. The `connect_to_db()` function establishes database connectivity using hardcoded connection parameters appropriate for the development environment. While this approach lacks the security and flexibility required for production systems, it eliminates configuration complexity that might distract from core learning objectives.

```python
def connect_to_db():
    print("Connecting to the PostgreSQL database")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5400,
            dbname="sf_311_calls_db",
            user="root",
            password="root"
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed {e}")
        raise
```

The `create_table()` function demonstrates DDL (Data Definition Language) operations within a Python context. The function creates both the schema and table if they don't exist, ensuring the pipeline can run successfully on fresh database instances. The inclusion of an `inserted_at` timestamp column with timezone specification (Africa/Nairobi) shows how to handle temporal data with explicit timezone awareness.

```python
def create_table(conn):
    print("Creating table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE TABLE IF NOT EXISTS bronze.sf_311_calls (
                service_request_id               TEXT PRIMARY KEY,
                requested_datetime               TEXT,
                closed_date                      TEXT,
                updated_datetime                 TEXT,
                status_description               TEXT,
                status_notes                     TEXT,
                agency_responsible               TEXT,
                service_name                     TEXT,
                service_subtype                  TEXT,
                service_details                  TEXT,
                address                          TEXT,
                street                           TEXT,
                supervisor_district              TEXT,
                neighborhoods_sffind_boundaries  TEXT,
                analysis_neighborhood            TEXT,
                police_district                  TEXT,
                lat                              TEXT,
                long                             TEXT,
                point                            TEXT,
                point_geom                       TEXT,
                source                           TEXT,
                media_url                        TEXT,
                bos_2012                         TEXT,
                data_as_of                       TEXT,
                inserted_at                      TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Africa/Nairobi')
            );
            """
        )
        conn.commit()
        print("Table created successfully!")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise
```

Data type conversion logic in the `convert_value()` function handles the heterogeneous nature of API responses. JSON objects and arrays are preserved using PostgreSQL's native JSON support, while other values are converted to strings. This approach maintains data fidelity while accommodating the varied data types present in the 311 calls dataset, particularly the geographic point data that arrives as nested JSON structures.

```python
def convert_value(value):
    """Convert values to appropriate types for PostgreSQL"""
    if isinstance(value, (dict, list)):
        return Json(value)
    elif value is None:
        return None
    else:
        return str(value)
```

The bulk insert implementation uses `execute_values()` for efficient batch processing. This approach significantly outperforms individual INSERT statements by reducing the number of database round trips and enabling PostgreSQL's batch processing optimizations. The transaction-based approach ensures data consistency, with automatic rollback if any errors occur during the insertion process.

```python
def insert_data(conn, data):
    print("Inserting raw data in the database...")
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO bronze.sf_311_calls (
        service_request_id, requested_datetime, closed_date, updated_datetime,
        status_description, status_notes, agency_responsible, service_name,
        service_subtype, service_details, address, street, supervisor_district,
        neighborhoods_sffind_boundaries, analysis_neighborhood, police_district,
        lat, long, point, point_geom, source, media_url, bos_2012, data_as_of
    ) VALUES %s
    ON CONFLICT (service_request_id) DO NOTHING
    """
    
    try:
        arg_list = []
        for record in data:
            args = tuple(convert_value(record.get(key)) for key in [
                'service_request_id', 'requested_datetime', 'closed_date', 'updated_datetime',
                'status_description', 'status_notes', 'agency_responsible', 'service_name',
                'service_subtype', 'service_details', 'address', 'street', 'supervisor_district',
                'neighborhoods_sffind_boundaries', 'analysis_neighborhood', 'police_district',
                'lat', 'long', 'point', 'point_geom', 'source', 'media_url', 'bos_2012',
                'data_as_of'
            ])
            arg_list.append(args)
        
        execute_values(cursor, insert_query, arg_list)
        conn.commit()
        print(f"Successfully inserted {len(data)} records!")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise
```

The main orchestration function demonstrates proper resource management through try-catch-finally blocks. Database connections are explicitly closed regardless of processing success or failure, preventing connection leaks that could exhaust database resources over time.

```python
def main():
    try:
        data = sample_api_data()
        conn = connect_to_db()
        create_table(conn)
        insert_data(conn, data)
    except Exception as e:
        print(f"An error occurred during execution{e}")
    finally:
        if "conn" in locals():
            conn.close()
            print("Database connection closed.")
```

## Data Exploration and Analysis Tools

The Jupyter notebook environment provides an interactive platform for data exploration that complements the automated pipeline components. The notebook imports both pandas and DuckDB, representing two different approaches to data analysis that serve complementary purposes in modern data workflows.

```python
import duckdb
import pandas as pd
from extract_data import sample_api_data

pd.set_option('display.max_columns', None)
conn = duckdb.connect()
```

Pandas excels at data manipulation, cleaning, and transformation tasks, providing intuitive APIs for common data operations. The `pd.set_option('display.max_columns', None)` configuration ensures complete column visibility when examining wide datasets like the 311 calls data, which contains 25 distinct fields.

DuckDB integration demonstrates the growing trend toward embedded analytical databases that provide SQL interfaces without requiring separate server infrastructure. DuckDB's columnar storage engine delivers superior performance for analytical queries compared to traditional row-based systems, while its seamless pandas integration allows for hybrid workflows that leverage the strengths of both tools.

The API-to-DataFrame conversion process showcases a fundamental pattern in data engineering: transforming semi-structured API responses into structured tabular data suitable for analysis. The `pd.DataFrame.from_records()` method efficiently converts the list of dictionaries returned by the Socrata API into a properly formatted DataFrame with appropriate column headers and data types.

```python
data = sample_api_data() 
results_df = pd.DataFrame.from_records(data)
```

## Development Workflow and Operational Considerations

The project's development workflow emphasizes simplicity and rapid iteration. The complete pipeline execution follows a straightforward sequence of commands that demonstrate the end-to-end process.

```bash
# Start infrastructure
docker compose up -d

# Run data extraction and loading
uv run python extract_data.py
uv run python insert_data.py

# Access web interface
# http://localhost:8080/ (pgAdmin)
```

The `docker compose up -d` command initializes the infrastructure in detached mode, allowing developers to focus on code development while the database services run in the background. The `uv run` commands execute Python scripts within the managed virtual environment, ensuring consistent dependency resolution across different development machines.

PgAdmin configuration requires understanding Docker's internal networking model. When establishing connections from the web interface, developers must use `postgres_container` as the hostname rather than `localhost`, since the connection originates from within the Docker network rather than the host system. This distinction illustrates important concepts about containerized application networking that apply broadly beyond this specific use case.

The hardcoded credential approach, while inappropriate for production systems, serves educational purposes by eliminating the complexity of secret management, environment variable configuration, and authentication systems. This allows learners to focus on data pipeline mechanics without getting distracted by operational security concerns that, while important, represent separate learning domains.

## Data Quality and Schema Considerations

The San Francisco 311 calls dataset presents interesting challenges and opportunities for understanding real-world data characteristics. The 25-column schema includes multiple timestamp fields that track request lifecycle events, geographic data in various formats, and hierarchical service classification systems that reflect the complexity of municipal operations.

Temporal data handling requires careful consideration of timezone implications, particularly when analyzing patterns across different time periods. The dataset includes both creation and modification timestamps, enabling analysis of service response times and operational efficiency metrics.

Geographic information appears in multiple formats within the dataset, including separate latitude and longitude fields, combined point representations, and structured GeoJSON objects. This redundancy reflects common patterns in government data systems where multiple applications require different spatial data formats, and demonstrates the importance of understanding data provenance and intended usage patterns.

The service classification hierarchy, with fields for service name, subtype, and details, illustrates how domain-specific taxonomies influence data structure design. These classifications enable analysis at different levels of granularity, from high-level service category trends to specific operational details about individual request types.

## Technology Stack Rationale and Trade-offs

The selected technology stack represents a balance between modern capabilities and educational accessibility. PostgreSQL provides industry-standard relational database functionality with excellent Python integration, while its widespread adoption ensures that skills developed with this system transfer readily to professional environments.

DuckDB's inclusion demonstrates the emerging category of embedded analytical databases that provide high-performance query processing without operational overhead. This technology choice illustrates how modern data stacks can incorporate specialized tools for specific use cases while maintaining overall system simplicity.

The Docker-based infrastructure approach teaches containerization concepts that have become fundamental to modern software deployment. By experiencing both the benefits (consistent environments, easy setup) and complexities (networking, volume management) of containerized systems, learners develop practical understanding of contemporary deployment patterns.

The decision to avoid authentication, comprehensive error handling, and production-ready configuration management reflects a pedagogical choice to focus on core data engineering concepts rather than operational concerns. This approach allows learners to understand fundamental patterns before adding the complexity layers required for production systems.

This implementation serves as a foundation for understanding how data moves through modern systems, from external APIs through processing pipelines to analytical databases, while maintaining enough simplicity to encourage experimentation and learning.
