# run_all_customers.py

import os
import sys
import logging
import subprocess
from datetime import datetime

# --- Configuration & Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

#CUSTOMERS = ['meydan', 'spc', 'shams', 'dubaisouth']
CUSTOMERS = ['meydan', 'spc', 'shams']

def run_customer_pipeline(customer):
    """Run the ETL pipeline for a single customer."""
    try:
        logging.info(f"Starting ETL for {customer.upper()}")
        result = subprocess.run([
            sys.executable, 'run_pipeline.py', customer
        ], capture_output=True, text=True, cwd=os.getcwd())
        
        if result.returncode == 0:
            logging.info(f"ETL for {customer.upper()} completed successfully")
            # Log stdout and stderr for details
            if result.stdout.strip():
                logging.info(f"ETL Stdout for {customer.upper()}:\n{result.stdout.strip()}")
            if result.stderr.strip():
                logging.info(f"ETL Details for {customer.upper()}:\n{result.stderr.strip()}")
            if not result.stdout.strip() and not result.stderr.strip():
                logging.info(f"No output captured for {customer.upper()}")
        else:
            logging.error(f"ETL for {customer.upper()} failed with code {result.returncode}")
            if result.stderr.strip():
                logging.error(f"Error Details: {result.stderr.strip()}")
            if result.stdout.strip():
                logging.info(f"Stdout before error: {result.stdout.strip()}")
            
    except Exception as e:
        logging.error(f"Exception running ETL for {customer}: {e}")

def main():
    """Run ETL pipeline for all customers sequentially."""
    logging.info("Starting scheduled ETL run for all customers")
    
    for customer in CUSTOMERS:
        run_customer_pipeline(customer)
        # Small delay between customers to reduce load
        import time
        time.sleep(5)  # 5 seconds between customers
    
    logging.info("Completed scheduled ETL run for all customers")

if __name__ == "__main__":
    main()