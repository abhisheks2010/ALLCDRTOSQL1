# cdr_ingestion.py

import json
import logging
import requests
from datetime import datetime, timedelta, timezone

import mysql.connector
from mysql.connector import errorcode

# Note: All os.getenv calls have been moved to the orchestrator (run_pipeline.py)
# This script now receives its configuration as a parameter.

def main(config):
    """
    Fetches CDRs and ingests them using the provided customer configuration.
    This function is called by the main run_pipeline.py script.

    Args:
        config (dict): A dictionary containing all necessary configuration
                       parameters for a specific customer.
    """
    # --- 1. Load Configuration ---
    # All variables are now read from the config object passed into the function.
    customer_name = config['name']
    api_base_url = config['api_base_url']
    api_jwt_token = config['api_jwt_token']
    api_account_id = config['api_account_id']
    db_name = config['db_name']
    db_host = config['db_host']
    db_user = config['db_user']
    db_password = config['db_password']
    fetch_interval_minutes = config['fetch_interval_minutes']
    initial_load_days = config.get('initial_load_days')
    api_page_size = 2000 # This can also be moved to config if it varies by customer

    logging.info(f"PHASE 1 (Ingestion) for '{customer_name.upper()}': Fetching data for the last {fetch_interval_minutes} minutes.")

    # --- 2. Set Up API and Database Connections ---
    end_time = datetime.now(timezone.utc)
    if initial_load_days:
        start_time = end_time - timedelta(days=int(initial_load_days))
        logging.info(f"Initial load mode: Fetching data for the last {initial_load_days} days.")
    else:
        start_time = end_time - timedelta(minutes=fetch_interval_minutes)
    end_date_unix = int(end_time.timestamp())
    start_date_unix = int(start_time.timestamp())

    db_connection = None
    start_key = None
    total_records_fetched = 0
    total_records_ingested = 0
    is_first_page = True

    try:
        logging.info(f"Connecting to MySQL database '{db_name}'...")
        db_connection = mysql.connector.connect(
            host=db_host, user=db_user, password=db_password, database=db_name
        )
        cursor = db_connection.cursor()
        logging.info("Database connection successful.")

        # --- 3. Paginate Through API Results ---
        while True:
            # Use seconds for Unix timestamps (not milliseconds)
            params = {'startDate': start_date_unix, 'endDate': end_date_unix, 'pageSize': api_page_size}
            if start_key:
                params['start_key'] = start_key
            headers = {'Authorization': f'Bearer {api_jwt_token}', 'x-account-id': api_account_id}

            req = requests.Request('GET', api_base_url, params=params)
            prepared_req = req.prepare()
            logging.info(f"Executing request to URL: {prepared_req.url}")

            response = requests.get(api_base_url, headers=headers, params=params, timeout=30)
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
        raise
    except mysql.connector.Error as err:
        logging.error(f"Database error occurred: {err}")
        if db_connection:
            db_connection.rollback()
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        raise
    finally:
        if db_connection and db_connection.is_connected():
            cursor.close()
            db_connection.close()
            logging.info("MySQL connection closed.")

        duplicates_skipped = total_records_fetched - total_records_ingested
        logging.info(f"Script finished. Total records ingested this run: {total_records_ingested}")
        logging.info(f"Duplicate records skipped: {duplicates_skipped}")


# This block is for standalone testing only. The script is intended to be called from run_pipeline.py
if __name__ == "__main__":
    logging.warning("This script is intended to be run from run_pipeline.py")
    logging.warning("To test, ensure your .env file has SHAMS_... variables set.")
    
    from dotenv import load_dotenv
    import os
    load_dotenv()
    
    # Example of creating a config for testing
    test_config = {
        "name": "shams_test",
        "api_base_url": os.getenv("SHAMS_API_BASE_URL"),
        "api_jwt_token": os.getenv("SHAMS_API_JWT_TOKEN"),
        "api_account_id": os.getenv("SHAMS_API_ACCOUNT_ID"),
        "db_name": os.getenv("SHAMS_DB_NAME"),
        "db_host": os.getenv("DB_HOST"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "fetch_interval_minutes": int(os.getenv("FETCH_INTERVAL_MINUTES", 5))
    }
    main(test_config)