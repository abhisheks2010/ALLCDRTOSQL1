# run_pipeline.py

import os
import sys
import logging
from dotenv import load_dotenv

# Import the main functions from your existing scripts
from cdr_ingestion import main as run_ingestion_phase
from etl_phase2 import main as run_transform_phase

# --- Configuration & Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_customer_config(customer_name):
    """
    Loads configuration for a specific customer from environment variables.
    """
    prefix = customer_name.upper()
    config = {
        "name": customer_name,
        "api_base_url": os.getenv(f"{prefix}_API_BASE_URL"),
        "api_jwt_token": os.getenv(f"{prefix}_API_JWT_TOKEN"),
        "api_account_id": os.getenv(f"{prefix}_API_ACCOUNT_ID"),
        "db_name": os.getenv(f"{prefix}_DB_NAME"),
        "db_host": os.getenv("DB_HOST"),
        "db_user": os.getenv("DB_USER"),
        "db_password": os.getenv("DB_PASSWORD"),
        "fetch_interval_minutes": int(os.getenv("FETCH_INTERVAL_MINUTES", 60))
    }
    
    # Validate that all required config values were found
    required_keys = ["api_base_url", "api_jwt_token", "api_account_id", "db_name", "db_host", "db_user"]
    if not all(config.get(key) for key in required_keys):
        logging.error(f"Missing one or more required configuration variables for customer: {customer_name}")
        sys.exit(1) # Exit with an error code
        
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