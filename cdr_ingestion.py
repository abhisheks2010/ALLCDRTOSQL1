# cdr_ingestion.py

import os
import json
import logging
import requests
from datetime import datetime, timedelta, timezone

import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

API_BASE_URL = "https://uc.ira-shams-sj.ucprem.voicemeetme.com:9443/api/v2/reports/cdrs/all"
API_JWT_TOKEN = os.getenv("API_JWT_TOKEN")
API_ACCOUNT_ID = os.getenv("API_ACCOUNT_ID")
API_PAGE_SIZE = 2000
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
FETCH_INTERVAL_MINUTES = int(os.getenv("FETCH_INTERVAL_MINUTES", 60))

def validate_config():
    """Checks if all required environment variables are set."""
    required_vars = ["API_JWT_TOKEN", "API_ACCOUNT_ID", "DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        logging.error(msg)
        raise ValueError(msg)
    logging.info("Configuration validated successfully.")

def main():
    """
    Fetches CDRs and ingests them, automatically skipping duplicates based on msg_id.
    """
    validate_config()

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=FETCH_INTERVAL_MINUTES)
    end_date_unix = int(end_time.timestamp())
    start_date_unix = int(start_time.timestamp())

    logging.info(f"Fetching CDR data for the last {FETCH_INTERVAL_MINUTES} minutes.")

    db_connection = None
    start_key = None
    total_records_fetched = 0
    total_records_ingested = 0
    is_first_page = True

    try:
        logging.info(f"Connecting to MySQL database '{DB_NAME}'...")
        db_connection = mysql.connector.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
        )
        cursor = db_connection.cursor()
        logging.info("Database connection successful.")

        while True:
            params = {'startDate': start_date_unix, 'endDate': end_date_unix, 'pageSize': API_PAGE_SIZE}
            if start_key:
                params['start_key'] = start_key
            headers = {'Authorization': f'Bearer {API_JWT_TOKEN}', 'x-account-id': API_ACCOUNT_ID}

            req = requests.Request('GET', API_BASE_URL, params=params)
            prepared_req = req.prepare()
            logging.info(f"Executing request to URL: {prepared_req.url}")

            response = requests.get(API_BASE_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()
            records = data.get('cdrs', [])

            if not records and is_first_page:
                logging.info("API call successful, but no new records were found.")
                break

            if records:
                page_record_count = len(records)
                total_records_fetched += page_record_count

                records_to_insert = []
                for record in records:
                    msg_id = record.get('msg_id')
                    if msg_id:
                        records_to_insert.append((msg_id, json.dumps(record)))
                    else:
                        logging.warning(f"Record found without a 'msg_id', cannot insert: {record}")

                if records_to_insert:
                    sql_insert_query = "INSERT IGNORE INTO cdr_raw_data (msg_id, record_data) VALUES (%s, %s)"
                    cursor.executemany(sql_insert_query, records_to_insert)
                    db_connection.commit()

                    newly_inserted_count = cursor.rowcount
                    total_records_ingested += newly_inserted_count
                    logging.info(f"Fetched {page_record_count} records. Inserted {newly_inserted_count} new records.")

            new_start_key = data.get('new_start_key')
            if not new_start_key:
                logging.info("Reached the last page of data.")
                break

            start_key = new_start_key
            is_first_page = False

    except requests.exceptions.RequestException as e:
        logging.error(f"API request failed: {e}")
        raise # Re-raise the exception to be caught by the pipeline runner
    except mysql.connector.Error as err:
        logging.error(f"Database error occurred: {err}")
        if db_connection:
            db_connection.rollback()
        raise # Re-raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise # Re-raise
    finally:
        if db_connection and db_connection.is_connected():
            cursor.close()
            db_connection.close()
            logging.info("MySQL connection closed.")

        duplicates_skipped = total_records_fetched - total_records_ingested
        logging.info(f"Script finished. Total records ingested this run: {total_records_ingested}")
        logging.info(f"Duplicate records skipped: {duplicates_skipped}")


if __name__ == "__main__":
    logging.info("Starting CDR ingestion script with duplicate handling...")
    main()