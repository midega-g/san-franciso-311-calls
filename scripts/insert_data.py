import psycopg2
from extract_data import sample_api_data
from psycopg2.extras import Json, execute_values
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
        lat, long, point, point_geom, source, media_url, bos_2012, data_as_of
    ) VALUES %s
    ON CONFLICT (service_request_id) DO NOTHING
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
main()