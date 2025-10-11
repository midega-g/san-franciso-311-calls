import psycopg2
import argparse
from datetime import datetime
from extract_data import sample_api_data
from psycopg2.extras import Json, execute_values
import os
import configparser
from dotenv import load_dotenv

# Load environment variables and configuration
load_dotenv()
config = configparser.ConfigParser()
config.read('config.conf')

def connect_to_db():
    print("Connecting to the PostgreSQL database")
    try:
        conn = psycopg2.connect(
            host=config['database']['host'],
            port=config['database']['port'],
            dbname=config['database']['name'],
            user=config['database']['user'],
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed {e}")
        raise
    
def create_table(conn):
    print("Creating table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE EXTENSION IF NOT EXISTS postgis;
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
                data_loaded_at                   TEXT,
                inserted_at                      TIMESTAMPTZ DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'Africa/Nairobi')
            );
            """
        )
        conn.commit()
        print("Table created successfully!")
        
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise


def convert_value(value):
        """Convert values to appropriate types for PostgreSQL"""
        if isinstance(value, (dict, list)):
            return Json(value)
        elif value is None:
            return None
        else:
            return str(value)



def insert_data(conn, data):
    print("Inserting raw data in the database...")
    cursor = conn.cursor()
    
    insert_query = """
    INSERT INTO bronze.sf_311_calls (
        service_request_id, requested_datetime, closed_date, updated_datetime,
        status_description, status_notes, agency_responsible, service_name,
        service_subtype, service_details, address, street, supervisor_district,
        neighborhoods_sffind_boundaries, analysis_neighborhood, police_district,
        lat, long, point, point_geom, source, media_url, bos_2012, data_as_of,
        data_loaded_at
    ) VALUES %s
    ON CONFLICT (service_request_id) 
    DO UPDATE SET
        requested_datetime = EXCLUDED.requested_datetime,
        closed_date = EXCLUDED.closed_date,
        updated_datetime = EXCLUDED.updated_datetime,
        status_description = EXCLUDED.status_description,
        status_notes = EXCLUDED.status_notes,
        agency_responsible = EXCLUDED.agency_responsible,
        service_name = EXCLUDED.service_name,
        service_subtype = EXCLUDED.service_subtype,
        service_details = EXCLUDED.service_details,
        address = EXCLUDED.address,
        street = EXCLUDED.street,
        supervisor_district = EXCLUDED.supervisor_district,
        neighborhoods_sffind_boundaries = EXCLUDED.neighborhoods_sffind_boundaries,
        analysis_neighborhood = EXCLUDED.analysis_neighborhood,
        police_district = EXCLUDED.police_district,
        lat = EXCLUDED.lat,
        long = EXCLUDED.long,
        point = EXCLUDED.point,
        point_geom = EXCLUDED.point_geom,
        source = EXCLUDED.source,
        media_url = EXCLUDED.media_url,
        bos_2012 = EXCLUDED.bos_2012,
        data_as_of = EXCLUDED.data_as_of,
        data_loaded_at = EXCLUDED.data_loaded_at,
        inserted_at = CURRENT_TIMESTAMP  -- Update timestamp on upsert
    """
    
    try:
        # Prepare list of tuples (one per record)
        arg_list = []
        for record in data:
            args = tuple(convert_value(record.get(key)) for key in [
                'service_request_id', 'requested_datetime', 'closed_date', 'updated_datetime',
                'status_description', 'status_notes', 'agency_responsible', 'service_name',
                'service_subtype', 'service_details', 'address', 'street', 'supervisor_district',
                'neighborhoods_sffind_boundaries', 'analysis_neighborhood', 'police_district',
                'lat', 'long', 'point', 'point_geom', 'source', 'media_url', 'bos_2012',
                'data_as_of', 'data_loaded_at'
            ])
            arg_list.append(args)
        
        execute_values(cursor, insert_query, arg_list)
        conn.commit()
        print(f"Successfully upserted {len(data)} records!")
        
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
        raise

def main():
    parser = argparse.ArgumentParser(
        description="""Fetch SF 311 service request data from the Socrata API (starting from the specified date up to the current timestamp) 
                       and upsert it into a PostgreSQL bronze table. Uses incremental updates to avoid duplicates based on service_request_id.""",
        formatter_class=argparse.RawDescriptionHelpFormatter,  # Preserves line breaks in description
        epilog="""Example Usage:
    python insert_data.py 2025 10 1
        # Fetches data from 2025-10-01T00:00:00.000 to now (e.g., up to 2025-10-10T[ current time ]).
        # Month/day are auto-zero-padded (e.g., month=9 becomes 09).

    python insert_data.py 2025 9 15
        # Fetches from 2025-09-15T00:00:00.000 to now. Use for custom ranges like September 15 or June 15.

Notes:
- Date range: Start date is inclusive (>=), end is exclusive (<= current UTC time). No LIMIT—fetches all matching records (may be large for broad ranges!).
- Valid inputs: Year (e.g., 2025), Month (1-12), Day (1-31). Invalid dates (e.g., Feb 30) will fail at API/query level.
- Database: Assumes localhost:5400 with user 'root'/pass 'root'. Adjust connect_to_db() if needed.
- Run 'python insert_data.py -h' for this help."""
    )
    parser.add_argument("year", type=int, help="""Start year (e.g., 2025). Must be a 4-digit integer in the valid range for SF 311 data (typically 2000+).""")
    parser.add_argument("month", type=int, help="""Start month (1-12, e.g., 10 for October, 9 for September). Single digits (1-9) are accepted—no need to pad with 0; the script handles it as 2025-09-01.""")
    parser.add_argument("day", type=int, help="""Start day of the month (1-31, e.g., 1 for the 1st, 15 for mid-month). Single digits (1-9) are accepted; auto-padded to 2025-10-01.""")
    
    args = parser.parse_args()
    
    min_start_str = f"{args.year}-{args.month:02d}-{args.day:02d}T00:00:00.000"
    
    conn = connect_to_db()
    try:
        create_table(conn)
        
        cursor = conn.cursor()
        # Get max updated_datetime for incremental
        cursor.execute("SELECT MAX(updated_datetime) FROM bronze.sf_311_calls")
        max_updated_result = cursor.fetchone()
        max_updated = max_updated_result[0] if max_updated_result and max_updated_result[0] else None
        
        # Get min requested_datetime to detect historical extension
        cursor.execute("SELECT MIN(requested_datetime) FROM bronze.sf_311_calls")
        min_requested_result = cursor.fetchone()
        existing_min = min_requested_result[0] if min_requested_result and min_requested_result[0] else None
        
        print(f"Existing min requested: {existing_min or 'None (empty table)'}")
        print(f"CLI min_start: {min_start_str}")
        print(f"Max updated_datetime from DB: {max_updated or 'None (full fetch)'}")
        
        # Decide filter: Historical full if extending back, else incremental
        updated_filter = None  # Default to full fetch for range
        is_historical = existing_min and min_start_str < existing_min
        if not is_historical:
            updated_filter = max_updated  # Use incremental if not extending history
        
        fetch_type = "historical full" if is_historical else "incremental"
        print(f"Fetch type: {fetch_type} (>= {min_start_str}{' + updated > ' + str(max_updated) if updated_filter else ''})")
        
        data = sample_api_data(min_start_str, updated_filter)
        
        # Debug: Warn if fetched many rows unexpectedly
        if updated_filter and len(data) > 100:
            print(f"Warning: Incremental run fetched {len(data)} rows—verify updated_datetime filter.")
        
        insert_data(conn, data)
        conn.commit()
        
    except Exception as e:
        print(f"An error occurred during execution: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()