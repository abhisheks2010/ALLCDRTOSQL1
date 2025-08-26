# run_pipeline.py

import logging

# Import the main functions from your existing scripts
from cdr_ingestion import main as run_ingestion_phase
from etl_phase2 import main as run_transform_phase

# Configure basic logging for the pipeline runner
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_full_etl_pipeline():
    """
    Executes the complete ETL pipeline sequentially.
    Phase 1: Ingests raw data from the API.
    Phase 2: Transforms raw data into the star schema.
    """
    try:
        logging.info("========================================")
        logging.info(" STARTING ETL PIPELINE RUN")
        logging.info("========================================")

        # --- Phase 1: Ingestion ---
        logging.info("Executing Phase 1: CDR Ingestion...")
        run_ingestion_phase()
        logging.info(" Phase 1: CDR Ingestion completed successfully.")

        # --- Phase 2: Transformation ---
        logging.info("Executing Phase 2: Data Transformation (ETL)...")
        run_transform_phase()
        logging.info(" Phase 2: Data Transformation completed successfully.")

        logging.info("========================================")
        logging.info(" ETL PIPELINE RUN FINISHED")
        logging.info("========================================")

    except Exception as e:
        logging.error(f" An error occurred during the ETL pipeline run: {e}", exc_info=True)
        # Depending on requirements, you could add notifications here (e.g., send an email)

if __name__ == "__main__":
    run_full_etl_pipeline()