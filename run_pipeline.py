# run_pipeline.py


import os
import sys
import logging
import requests
import time
from dotenv import load_dotenv

# Import the main functions from your existing scripts
from cdr_ingestion import main as run_ingestion_phase
from etl_phase2 import main as run_transform_phase

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_RETRIES = 3

def acquire_token(auth_base_url, username, password, account_id, tenant):
    """
    Fetch an access token using multiple candidate endpoints, similar to the JS getPortalToken.
    Tries modern endpoints first, falls back to older ones.
    """
    base = auth_base_url.rstrip('/')
    candidates = [
        # OAuth login path used by the portal UI (works on modern installs)
        {"url": f"{base}/api/v2/config/login/oauth", "body": {"domain": tenant, "username": username, "password": password}},
        # v2 login using domain (fallback for older back-ends)
        {"url": f"{base}/api/v2/login", "body": {"domain": tenant, "username": username, "password": password}},
        # very old legacy login path
        {"url": f"{base}/api/login", "body": {"domain": tenant, "username": username, "password": password}},
    ]

    for candidate in candidates:
        url = candidate["url"]
        body = candidate["body"]
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(url, json=body, timeout=5)
                response.raise_for_status()
                data = response.json()
                access = data.get("accessToken") or data.get("access_token")
                if access:
                    logging.info(f"✅ Portal login succeeded at {url}")
                    return access
                else:
                    raise ValueError("No access token in response")
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    logging.warning(f"Login failed at {url}: {e}")
                    break  # Try next candidate
                delay = 1000 * (2 ** attempt)  # Exponential backoff in ms, but sleep in seconds
                time.sleep(delay / 1000)
    raise Exception("All portal login attempts failed – check credentials/endpoints")

def get_customer_config(customer_name):
    """
    Loads configuration for a specific customer from environment variables and acquires a fresh JWT token.
    """
    prefix = customer_name.upper()
    config = {
        "name": customer_name,
        "base_url": os.getenv(f"{prefix}_BASE_URL"),
        "api_base_url": os.getenv(f"{prefix}_API_BASE_URL"),
        "api_username": os.getenv(f"{prefix}_API_USERNAME"),
        "api_password": os.getenv(f"{prefix}_API_PASSWORD"),
        "api_account_id": os.getenv(f"{prefix}_API_ACCOUNT_ID"),
        "api_tenant": os.getenv(f"{prefix}_API_TENANT"),
        "db_name": os.getenv(f"{prefix}_DB_NAME"),
        "db_host": os.getenv("DB_HOST"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "fetch_interval_minutes": int(os.getenv("FETCH_INTERVAL_MINUTES", 60)),
        "initial_load_days": os.getenv("INITIAL_LOAD_DAYS")
    }
    
    # Validate that all required config values were found
    required_keys = ["base_url", "api_base_url", "api_username", "api_password", "api_account_id", "api_tenant", "db_name", "db_host", "db_user"]
    if not all(config.get(key) for key in required_keys):
        logging.error(f"Missing one or more required configuration variables for customer: {customer_name}")
        sys.exit(1) # Exit with an error code
    
    # Acquire token dynamically
    try:
        config["api_jwt_token"] = acquire_token(
            config["base_url"],
            config["api_username"],
            config["api_password"],
            config["api_account_id"],
            config["api_tenant"]
        )
    except Exception as e:
        logging.error(f"Failed to acquire JWT token for {customer_name}: {e}")
        sys.exit(1)

    return config

def run_full_etl_pipeline(config):
    """
    Executes the complete ETL pipeline sequentially using the provided configuration.
    """
    customer = config['name']
    try:
        logging.info("="*40)
        logging.info(f"🚀 STARTING ETL PIPELINE RUN FOR CUSTOMER: {customer.upper()}")
        logging.info("="*40)

        # Pass the config object to each phase
        run_ingestion_phase(config)
        run_transform_phase(config)

        logging.info("="*40)
        logging.info(f"🎉 ETL PIPELINE RUN FOR {customer.upper()} FINISHED SUCCESSFULLY")
        logging.info("="*40)

    except Exception as e:
        logging.error(f"❌ An error occurred during the ETL pipeline run for {customer.upper()}: {e}", exc_info=True)

if __name__ == "__main__":
    # Expect a customer name ('shams' or 'spc') as a command-line argument
    if len(sys.argv) < 2:
        logging.error("Usage: python run_pipeline.py <customer_name>")
        sys.exit(1)
        
    customer_arg = sys.argv[1].lower()
    customer_config = get_customer_config(customer_arg)
    
    run_full_etl_pipeline(customer_config)