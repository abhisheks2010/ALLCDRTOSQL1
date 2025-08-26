# etl_phase2.py

import os
import json
import logging
from datetime import datetime
import phonenumbers

import mysql.connector
from dotenv import load_dotenv

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Configuration ---
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

DIM_CACHE = {
    "users": {}, "call_disposition": {}, "system": {}, "campaigns": {}, "queues": {},
    "date": {}, "time_of_day": {}
}

def get_db_connection():
    """Establishes and returns a database connection."""
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME
    )

def get_or_create_key(cursor, table, unique_columns, value_map):
    """
    Generic function to look up or create a record in a dimension table.
    Returns the primary key.
    """
    cache_key_parts = []
    for col in sorted(unique_columns.keys()):
        cache_key_parts.append(str(unique_columns.get(col, 'NULL')))
    cache_key = f"{table}_{'_'.join(cache_key_parts)}"
    
    if cache_key in DIM_CACHE[table]:
        return DIM_CACHE[table][cache_key]

    query_parts = []
    query_values = []
    for col, val in unique_columns.items():
        if val is None:
            query_parts.append(f"`{col}` IS NULL")
        else:
            query_parts.append(f"`{col}` = %s")
            query_values.append(val)
            
    # 💡 FINAL FIX: Handle special primary key names for specific tables
    pk_name = f"{table.rstrip('s')}_key"
    if table == "time_of_day":
        pk_name = "time_key"
    elif table == "call_disposition":
        pk_name = "disposition_key" # <-- ADDED THIS CONDITION
        
    query = f"SELECT `{pk_name}` FROM `dim_{table}` WHERE " + " AND ".join(query_parts)
    cursor.execute(query, tuple(query_values))
    result = cursor.fetchone()

    if result:
        key = result[0]
    else:
        cols = ", ".join([f"`{k}`" for k in value_map.keys()])
        placeholders = ", ".join(["%s"] * len(value_map))
        insert_query = f"INSERT INTO `dim_{table}` ({cols}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(value_map.values()))
        key = cursor.lastrowid
    
    DIM_CACHE[table][cache_key] = key
    return key

def process_cdr(cursor, cdr_json):
    """
    Transforms a single raw CDR JSON object and loads it into the star schema.
    """
    interaction_time_micro = cdr_json.get("interaction_time", 0)
    ts = datetime.fromtimestamp(interaction_time_micro / 1000000)
    
    date_key = int(ts.strftime('%Y%m%d'))
    time_key = int(ts.strftime('%H%M%S'))
    
    get_or_create_key(cursor, "date", {"date_key": date_key}, {
        "date_key": date_key, "full_date": ts.date(), "year": ts.year, 
        "quarter": (ts.month - 1) // 3 + 1, "month": ts.month, "day_of_week": ts.strftime('%A')
    })
    get_or_create_key(cursor, "time_of_day", {"time_key": time_key}, {
        "time_key": time_key, "full_time": ts.time(), "hour": ts.hour, "minute": ts.minute
    })

    def get_user_key(number, name):
        if not number: return None
        country_code, country_name = None, "Unknown"
        try:
            # Handle non-standard numbers gracefully
            if not str(number).strip().isdigit() and not str(number).startswith('+'):
                 raise phonenumbers.phonenumberutil.NumberParseException(0, "Invalid number format")

            if not number.startswith('+'):
                number_with_plus = f"+{number}"
            else:
                number_with_plus = number
            
            parsed_number = phonenumbers.parse(number_with_plus, None)
            if parsed_number:
                country_code = parsed_number.country_code
        except phonenumbers.phonenumberutil.NumberParseException:
            logging.warning(f"Could not parse phone number: {number}")

        return get_or_create_key(cursor, "users", {"user_number": number}, {
            "user_number": number, "user_name": name, "country_code": country_code, "country_name": country_name
        })
    caller_user_key = get_user_key(cdr_json.get('caller_id_number'), cdr_json.get('caller_id_name'))
    callee_user_key = get_user_key(cdr_json.get('callee_id_number'), cdr_json.get('callee_id_name'))

    disposition_key = get_or_create_key(cursor, "call_disposition", {
        "call_direction": cdr_json.get("call_direction"), "hangup_cause": cdr_json.get("hangup_cause"),
        "disposition": cdr_json.get("disposition"), "subdisposition": cdr_json.get("subdisposition")
    }, {
        "call_direction": cdr_json.get("call_direction"), "hangup_cause": cdr_json.get("hangup_cause"),
        "disposition": cdr_json.get("disposition"), "subdisposition": cdr_json.get("subdisposition")
    })

    system_key = get_or_create_key(cursor, "system", {"switch_hostname": cdr_json.get("node")}, {
        "switch_hostname": cdr_json.get("node"), "app_name": cdr_json.get("app_name"), 
        "realm": cdr_json.get("custom_channel_vars", {}).get("realm")
    }) if cdr_json.get("node") else None
    
    campaign_key = get_or_create_key(cursor, "campaigns", {"campaign_id": cdr_json.get("campaign_id")}, {
        "campaign_id": cdr_json.get("campaign_id"), "campaign_name": cdr_json.get("campaign_name")
    }) if cdr_json.get("campaign_id") else None

    queue_key = get_or_create_key(cursor, "queues", {"queue_id": cdr_json.get("queue_id")}, {
        "queue_id": cdr_json.get("queue_id"), "queue_name": cdr_json.get("queue_name")
    }) if cdr_json.get("queue_id") else None

    fact_call_data = {
        "msg_id": cdr_json.get("msg_id"), "call_id": cdr_json.get("call_id"),
        "date_key": date_key, "time_key": time_key,
        "caller_user_key": caller_user_key, "callee_user_key": callee_user_key,
        "disposition_key": disposition_key, "system_key": system_key,
        "campaign_key": campaign_key, "queue_key": queue_key,
        "duration_seconds": cdr_json.get("duration_seconds"), "billing_seconds": cdr_json.get("billing_seconds"),
        "call_recording_url": cdr_json.get("media_name"), "is_conference": cdr_json.get("is_conference", False)
    }

    cols = ", ".join([f"`{k}`" for k in fact_call_data.keys()])
    placeholders = ", ".join(["%s"] * len(fact_call_data))
    
    insert_query = f"INSERT IGNORE INTO `fact_calls` ({cols}) VALUES ({placeholders})"
    cursor.execute(insert_query, tuple(fact_call_data.values()))
    
    if "agent_history" in cdr_json and cdr_json.get("agent_history"):
        logging.info(f"Processing agent history for call_id {cdr_json.get('call_id')} (not yet implemented).")

def main():
    """Main ETL process function."""
    db_conn = None
    try:
        db_conn = get_db_connection()
        cursor = db_conn.cursor()

        cursor.execute("SELECT id, record_data FROM cdr_raw_data WHERE etl_processed_at IS NULL LIMIT 1000")
        raw_records = cursor.fetchall()
        
        if not raw_records:
            logging.info("No new raw CDRs to process.")
            return

        logging.info(f"Found {len(raw_records)} new CDRs to process.")
        processed_count = 0
        
        for raw_id, record_data in raw_records:
            try:
                cdr_json = json.loads(record_data)
                process_cdr(cursor, cdr_json)
                update_query = "UPDATE cdr_raw_data SET etl_processed_at = %s WHERE id = %s"
                cursor.execute(update_query, (datetime.now(), raw_id))
                processed_count += 1
            except Exception as e:
                logging.error(f"Failed to process raw record ID {raw_id}: {e}", exc_info=False)
                fail_timestamp = datetime(1971, 1, 1)
                fail_query = "UPDATE cdr_raw_data SET etl_processed_at = %s WHERE id = %s"
                cursor.execute(fail_query, (fail_timestamp, raw_id))
        
        db_conn.commit()
        logging.info(f"ETL process completed. Successfully processed {processed_count}/{len(raw_records)} records.")

    except mysql.connector.Error as err:
        logging.error(f"Database error during ETL process: {err}")
        if db_conn:
            db_conn.rollback()
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()

if __name__ == "__main__":
    main()