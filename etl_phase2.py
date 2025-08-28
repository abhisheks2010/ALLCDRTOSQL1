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

# A cache to store dimension keys in memory to reduce DB lookups during a single run.
# This is reset for each run.
DIM_CACHE = {
    "users": {}, "call_disposition": {}, "system": {}, "campaigns": {}, "queues": {},
    "date": {}, "time_of_day": {}
}

def get_db_connection(config):
    """Establishes and returns a database connection using the provided config."""
    return mysql.connector.connect(
        host=config['db_host'],
        user=config['db_user'],
        password=config['db_password'],
        database=config['db_name']
    )

def get_or_create_key(cursor, table, unique_columns, value_map, is_agent_update=False):
    """
    Generic function to look up or create a record in a dimension table.
    Returns the primary key.
    """
    # Create a consistent cache key from the unique columns
    cache_key_parts = []
    for col in sorted(unique_columns.keys()):
        cache_key_parts.append(str(unique_columns.get(col, 'NULL')))
    cache_key = f"{table}_{'_'.join(cache_key_parts)}"

    # Return from cache if available
    if cache_key in DIM_CACHE[table]:
        cached_data = DIM_CACHE[table][cache_key]
        if is_agent_update and not cached_data.get('is_agent', False):
            logging.info(f"Updating user '{unique_columns.get('user_number')}' to is_agent=True.")
            cursor.execute("UPDATE dim_users SET is_agent = TRUE WHERE user_key = %s", (cached_data['key'],))
            DIM_CACHE[table][cache_key]['is_agent'] = True
        return cached_data['key']

    # --- Database Lookup ---
    query_parts = []
    query_values = []
    for col, val in unique_columns.items():
        if val is None:
            query_parts.append(f"`{col}` IS NULL")
        else:
            query_parts.append(f"`{col}` = %s")
            query_values.append(val)
            
    pk_name = f"{table.rstrip('s')}_key"
    if table == "time_of_day": pk_name = "time_key"
    elif table == "call_disposition": pk_name = "disposition_key"
    
    select_cols = f"`{pk_name}`"
    if table == "users":
        select_cols += ", `is_agent`"

    query = f"SELECT {select_cols} FROM `dim_{table}` WHERE " + " AND ".join(query_parts)
    cursor.execute(query, tuple(query_values))
    result = cursor.fetchone()

    if result:
        key = result[0]
        if table == "users":
            is_agent_status = result[1]
            if is_agent_update and not is_agent_status:
                 logging.info(f"Updating user '{unique_columns.get('user_number')}' to is_agent=True.")
                 cursor.execute("UPDATE dim_users SET is_agent = TRUE WHERE user_key = %s", (key,))
                 is_agent_status = True
            DIM_CACHE[table][cache_key] = {'key': key, 'is_agent': is_agent_status}
        else:
            DIM_CACHE[table][cache_key] = {'key': key}
    else:
        for col, val in unique_columns.items():
            if col not in value_map:
                value_map[col] = val
        
        if table == "users":
             value_map['is_agent'] = is_agent_update

        cols = ", ".join([f"`{k}`" for k in value_map.keys()])
        placeholders = ", ".join(["%s"] * len(value_map))
        insert_query = f"INSERT INTO `dim_{table}` ({cols}) VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(value_map.values()))
        key = cursor.lastrowid
        
        if table == "users":
            DIM_CACHE[table][cache_key] = {'key': key, 'is_agent': is_agent_update}
        else:
             DIM_CACHE[table][cache_key] = {'key': key}

    return key

def process_agent_history(cursor, cdr_json, call_key):
    """
    Processes the agent_history array and populates the fact_agent_legs table.
    Also updates the is_agent flag in dim_users.
    """
    fono_uc_data = cdr_json.get("fonoUC", {})
    cc_data = fono_uc_data.get("cc", fono_uc_data.get("cc_outbound", {}))
    agent_history = cc_data.get("agent_history", [])

    if not agent_history:
        return 0

    processed_legs = 0
    agent_events = {}
    for event in agent_history:
        agent_ext = event.get("ext")
        if not agent_ext:
            continue
        
        if agent_ext not in agent_events:
            agent_events[agent_ext] = {}
        
        event_type = event.get("event")
        if event_type == "dialing" or event_type == "agent_outbound":
             agent_events[agent_ext]["dial_time"] = event.get("called_time")
        elif event_type == "answer":
             agent_events[agent_ext]["answer_time"] = event.get("answered_time")

    hangup_time = cc_data.get("hangup_time")

    for agent_ext, times in agent_events.items():
        try:
            agent_key = get_user_key(cursor, agent_ext, None, is_agent_update=True)
            if not agent_key:
                logging.warning(f"Could not find or create user for agent extension: {agent_ext}")
                continue

            dial_time = times.get("dial_time")
            answer_time = times.get("answer_time")
            
            wait_seconds = int(answer_time - dial_time) if answer_time and dial_time else 0
            talk_seconds = int(hangup_time - answer_time) if hangup_time and answer_time else 0
            
            wrap_up_seconds = 0

            fact_data = {
                "call_key": call_key, "agent_key": agent_key, "disposition_key": None,
                "wait_seconds": wait_seconds, "talk_seconds": talk_seconds, "wrap_up_seconds": wrap_up_seconds
            }

            cols = ", ".join([f"`{k}`" for k in fact_data.keys()])
            placeholders = ", ".join(["%s"] * len(fact_data))
            insert_query = f"INSERT INTO `fact_agent_legs` ({cols}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(fact_data.values()))
            processed_legs += 1
        except Exception as e:
            logging.error(f"Failed to process agent leg for ext {agent_ext} in call_id {cdr_json.get('call_id')}: {e}")

    return processed_legs

def get_user_key(cursor, number, name, is_agent_update=False):
    """Helper function to get or create a user key."""
    if not number: return None
    country_code, country_name = None, "Unknown"
    number_to_parse = str(number).strip()

    if number_to_parse.isdigit() and len(number_to_parse) == 4:
        country_code = None
        country_name = "Internal"
    else:
        try:
            parsed_number = phonenumbers.parse(number_to_parse, "AE")
            country_code = parsed_number.country_code
        except phonenumbers.phonenumberutil.NumberParseException:
            logging.warning(f"Could not parse phone number: {number}")

    return get_or_create_key(cursor, "users", {"user_number": number}, {
        "user_name": name, "country_code": country_code, "country_name": country_name
    }, is_agent_update=is_agent_update)

def process_cdr(cursor, cdr_json):
    """
    Transforms a single raw CDR JSON object and loads it into the star schema.
    """
    # --- 1. Extract and Transform Data ---
    interaction_time_micro = cdr_json.get("interaction_time", 0)
    ts = datetime.fromtimestamp(interaction_time_micro / 1000000)
    date_key = int(ts.strftime('%Y%m%d'))
    time_key = int(ts.strftime('%H%M%S'))
    
    fono_uc_data = cdr_json.get("fonoUC", {})
    disposition = fono_uc_data.get("disposition") or cdr_json.get("disposition")
    follow_up_notes = fono_uc_data.get("follow_up_notes")
    subdisposition_1 = None
    subdisposition_2 = None

    subdisp_obj = fono_uc_data.get("subdisposition")
    if isinstance(subdisp_obj, dict):
        subdisposition_1 = subdisp_obj.get("name")
        nested_subdisp_obj = subdisp_obj.get("subdisposition")
        if isinstance(nested_subdisp_obj, dict):
            subdisposition_2 = nested_subdisp_obj.get("name")
    elif isinstance(subdisp_obj, str):
        subdisposition_1 = subdisp_obj
    else:
        subdisposition_1 = cdr_json.get("subdisposition")

    # --- 2. Handle Dimensions (Get or Create Keys) ---
    get_or_create_key(cursor, "date", {"date_key": date_key}, {
        "full_date": ts.date(), "year": ts.year, 
        "quarter": (ts.month - 1) // 3 + 1, "month": ts.month, "day_of_week": ts.strftime('%A')
    })
    get_or_create_key(cursor, "time_of_day", {"time_key": time_key}, {
        "full_time": ts.time(), "hour": ts.hour, "minute": ts.minute
    })

    caller_user_key = get_user_key(cursor, cdr_json.get('caller_id_number'), cdr_json.get('caller_id_name'))
    callee_user_key = get_user_key(cursor, cdr_json.get('callee_id_number'), cdr_json.get('callee_id_name'))

    disposition_key = get_or_create_key(cursor, "call_disposition", {
        "call_direction": cdr_json.get("call_direction"), 
        "hangup_cause": cdr_json.get("hangup_cause"),
        "disposition": disposition, 
        "subdisposition_1": subdisposition_1,
        "subdisposition_2": subdisposition_2
    }, {})

    system_key = get_or_create_key(cursor, "system", {"switch_hostname": cdr_json.get("node")}, {
        "app_name": cdr_json.get("app_name"), 
        "realm": cdr_json.get("custom_channel_vars", {}).get("realm")
    }) if cdr_json.get("node") else None
    
    # --- NEW: LOGIC TO EXTRACT NESTED QUEUE INFO ---
    queue_key = None
    # Data can be in fonoUC.cc or fonoUC.cc_outbound
    cc_data = fono_uc_data.get("cc", fono_uc_data.get("cc_outbound", {}))
    queue_name = cc_data.get("queue_name")
    if queue_name:
        # Since we don't have a queue_id, we will use the queue_name as the unique identifier.
        # This assumes queue names are unique.
        queue_key = get_or_create_key(cursor, "queues", 
            {"queue_name": queue_name}, # Use name as the unique key
            {"queue_id": queue_name}    # Populate queue_id with the name as well
        )
    # --- END OF NEW LOGIC ---
    
    # Campaign info is not present in the data, so campaign_key will remain None
    campaign_key = get_or_create_key(cursor, "campaigns", {"campaign_id": cdr_json.get("campaign_id")}, {
        "campaign_name": cdr_json.get("campaign_name")
    }) if cdr_json.get("campaign_id") else None

    # --- 3. Load Fact Table ---
    fact_call_data = {
        "msg_id": cdr_json.get("msg_id"), "call_id": cdr_json.get("call_id"),
        "date_key": date_key, "time_key": time_key,
        "caller_user_key": caller_user_key, "callee_user_key": callee_user_key,
        "disposition_key": disposition_key, "system_key": system_key,
        "campaign_key": campaign_key, "queue_key": queue_key, # This will now be populated
        "duration_seconds": cdr_json.get("duration_seconds"), 
        "billing_seconds": cdr_json.get("billing_seconds"),
        "call_recording_url": cdr_json.get("media_name"), 
        "is_conference": cdr_json.get("is_conference", False),
        "follow_up_notes": follow_up_notes
    }

    cols = ", ".join([f"`{k}`" for k in fact_call_data.keys()])
    placeholders = ", ".join(["%s"] * len(fact_call_data))
    
    insert_query = f"INSERT IGNORE INTO `fact_calls` ({cols}) VALUES ({placeholders})"
    cursor.execute(insert_query, tuple(fact_call_data.values()))
    
    if cursor.rowcount > 0:
        call_key = cursor.lastrowid
        process_agent_history(cursor, cdr_json, call_key)

def main(config):
    """Main ETL process function using the provided customer configuration."""
    customer_name = config['name']
    logging.info(f"PHASE 2 (Transform) for '{customer_name.upper()}': Processing raw data.")

    db_conn = None
    try:
        db_conn = get_db_connection(config)
        cursor = db_conn.cursor()

        while True:
            cursor.execute("SELECT id, record_data FROM cdr_raw_data WHERE etl_processed_at IS NULL LIMIT 500")
            raw_records = cursor.fetchall()
            
            if not raw_records:
                logging.info("No new raw CDRs to process.")
                break

            logging.info(f"Found {len(raw_records)} new CDRs to process in this batch.")
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
            logging.info(f"Batch completed. Successfully processed {processed_count}/{len(raw_records)} records.")

    except mysql.connector.Error as err:
        logging.error(f"Database error during ETL process: {err}")
        if db_conn:
            db_conn.rollback()
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()

if __name__ == "__main__":
    logging.warning("This script is intended to be run from run_pipeline.py")
    logging.warning("To test, ensure your .env file has SHAMS_... variables set.")
    
    test_config = {
        "name": "shams_test",
        "db_name": os.getenv("SHAMS_DB_NAME"),
        "db_host": os.getenv("DB_HOST"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
    }
    main(test_config)