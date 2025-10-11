#!/usr/bin/env python

from datetime import datetime, timezone
from sodapy import Socrata
import time
import os
import configparser
from dotenv import load_dotenv

# Load environment variables and configuration
load_dotenv()
config = configparser.ConfigParser()
config.read('config.conf')

# Initialize Socrata client with configuration
client = Socrata(
    config['api']['socrata_domain'],
    os.getenv('SOCRATA_TOKEN'),
    username=config['api']['socrata_username'],
    password=os.getenv('SOCRATA_PASSWORD')
)


def sample_api_data(min_start_str, updated_filter=None):
    # Get current date/time as end (up to now for precision) - USE UTC
    now = datetime.now(timezone.utc)
    # Truncate to milliseconds
    end_date_str = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]

    # Build where clause: Always filter on requested_datetime >= min_start
    where = f'requested_datetime >= "{min_start_str}"'
    if updated_filter:
        where += f' AND updated_datetime > "{updated_filter}"'

    print(f"Fetching new/updated records (requested >= {min_start_str}, updated > {updated_filter or 'initial'}) to {end_date_str}...")

    # Pagination parameters
    limit = 1000  # Rows per page; max 50,000 allowed
    offset = 0
    all_results = []  # Accumulate all pages here

    while True:
        try:
            # Fetch one page using 'where' parameter for SoQL filter (avoids conflict with limit/offset)
            page_results = client.get(
                config['api']['dataset_id'],
                where=where,
                limit=limit,
                offset=offset
            )

            # Append this page to the full list
            all_results.extend(page_results)

            print(
                f"Fetched {len(page_results)} rows (total so far: {len(all_results)})")

            # If fewer rows than limit, we've reached the end
            if len(page_results) < limit:
                break

            # Next page after a 5-second rest to respect rate limits
            time.sleep(5)
            offset += limit
        except Exception as e:
            print(f"Error fetching data: {e}.")
            time.sleep(10)

    print(f"Complete: {len(all_results)} total records fetched.")
    return all_results