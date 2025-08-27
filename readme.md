Python ETL Pipeline for CDR API to MySQL Data Warehouse
📝 Objective
This project provides a robust, multi-tenant, two-phase ETL (Extract, Transform, Load) pipeline in Python. It is designed to fetch Call Detail Records (CDRs) from multiple customer REST APIs, load the raw data into dedicated staging tables, and then transform it into a structured star schema within a MySQL database, making it ready for analytics and business intelligence.

The pipeline is designed to be executed on a recurring schedule (e.g., every 1-5 minutes) and handles API pagination, duplicate records, and complex data transformations automatically for each customer.

📂 Code Structure
The project is organized into the following files:

run_pipeline.py: (Main Script) The master script used to execute the entire ETL pipeline for a specific customer.

cdr_ingestion.py: The script for Phase 1 (Extract), responsible for fetching raw data from the API and loading it into a staging table.

etl_phase2.py: The script for Phase 2 (Transform & Load), which processes the raw data and populates the final star schema.

requirements.txt: A list of all the Python libraries required to run the pipeline.

.env: A configuration file for storing all secrets and settings for all customers.

schema_phase2.sql: The complete SQL script to create all final dimension and fact tables for a single customer's data warehouse.

🚀 Getting Started
Follow these steps to set up and run the ETL pipeline.

Prerequisites
Python 3.8 or newer.

Access to a MySQL server (local or remote).

MySQL client tools (e.g., MySQL Workbench, mysql CLI).

1. Database and User Setup
For data isolation, each customer must have their own separate database.

Connect to your MySQL server as an administrator (root).

Create a database for each customer. For example:

SQL

CREATE DATABASE allcdr_shams;
CREATE DATABASE allcdr_spc;
Create a dedicated user for the script to enhance security:

SQL

CREATE USER 'your_db_user'@'localhost' IDENTIFIED BY 'your_db_password';
-- Grant privileges for both databases
GRANT ALL PRIVILEGES ON allcdr_shams.* TO 'your_db_user'@'localhost';
GRANT ALL PRIVILEGES ON allcdr_spc.* TO 'your_db_user'@'localhost';
FLUSH PRIVILEGES;
For each new database, run the contents of the schema_phase2.sql file to create all the necessary tables. Make sure to change the USE statement at the top of the script for each customer before running it (e.g., USE allcdr_shams; for the first run, then USE allcdr_spc; for the second).

2. Installation
Clone the repository and switch to the correct branch:

Bash

git clone https://github.com/abhisheks2010/ALLCDRTOSQL1.git
cd ALLCDRTOSQL1
git checkout multiCustomer
Create and activate a Python virtual environment:

Bash

# Create the environment
python3 -m venv venv

# Activate on Windows
.\venv\Scripts\activate

# Activate on Linux/macOS
source venv/bin/activate
Install the required libraries:

Bash

pip install -r requirements.txt
3. Configuration
Create a copy of the .env.example file and rename it to .env.

Open the new .env file and fill in the credentials for all your customers, using a unique prefix for each one.

Ini, TOML

# .env - Multi-Customer Configuration

# --- Customer 1: SHAMS Configuration ---
SHAMS_API_BASE_URL="https://uc.ira-shams-sj.ucprem.voicemeetme.com:9443/api/v2/reports/cdrs/all"
SHAMS_API_JWT_TOKEN="your_shams_jwt_token_here"
SHAMS_API_ACCOUNT_ID="your_shams_account_id_here"
SHAMS_DB_NAME="allcdr_shams"

# --- Customer 2: SPC Configuration ---
SPC_API_BASE_URL="https://ira-spc-sj.ucprem.voicemeetme.com:9443/api/v2/reports/cdrs/all"
SPC_API_JWT_TOKEN="your_spc_jwt_token_here"
SPC_API_ACCOUNT_ID="your_spc_account_id_here"
SPC_DB_NAME="allcdr_spc"

# --- Common Configuration ---
FETCH_INTERVAL_MINUTES=5
DB_HOST="localhost"
DB_USER="your_db_user"
DB_PASSWORD="your_db_password"
▶️ How to Run the Pipeline
To run the entire ETL process, you must now provide the customer's name as a command-line argument. This name should match the prefix used in the .env file (case-insensitive).

Make sure your virtual environment is activated.

Execute the run_pipeline.py script from your terminal:

Bash

# To run the pipeline for the SHAMS customer
python run_pipeline.py shams

# To run the pipeline for the SPC customer
python run_pipeline.py spc
⚙️ Scheduling the Pipeline
For automation, you must create a separate scheduled task for each customer.

Scheduling with Cron (Linux/macOS)
Open your crontab file for editing: crontab -e

Add a line for each customer. This example runs the pipeline every 5 minutes for both customers.

Code snippet

# Run the SHAMS CDR ETL pipeline every 5 minutes
*/5 * * * * /path/to/project/venv/bin/python /path/to/project/run_pipeline.py shams

# Run the SPC CDR ETL pipeline every 5 minutes
*/5 * * * * /path/to/project/venv/bin/python /path/to/project/run_pipeline.py spc
Scheduling with Windows Task Scheduler
Open Task Scheduler and create a separate task for each customer.

The Program/script and Start in fields will be the same for each task.

The Add arguments (optional) field is where you specify the customer:

SHAMS Task Arguments: run_pipeline.py shams

SPC Task Arguments: run_pipeline.py spc

Data Handling and Business Logic
The pipeline includes several key features to ensure data quality and integrity:

Duplicate Handling: Resilience against duplicate data is achieved at multiple levels. The cdr_raw_data staging table and the final fact_calls table both have a UNIQUE index on the msg_id column. Additionally, the ingestion script uses an INSERT IGNORE command.

Nested JSON Parsing: The ETL script is designed to parse complex, nested JSON objects. It specifically extracts detailed agent disposition information from the fonoUC object, including:

Follow-up Notes: Captures manual notes left by agents.

Nested Subdispositions: Handles up to two levels of subdispositions (e.g., Sale -> Lead Converted -> Manually Assigned).

Robust Phone Number Parsing: The script contains intelligent logic to handle a variety of phone number formats:

It correctly identifies and processes standard international numbers (e.g., +971...).

It handles local UAE numbers by defaulting to the "AE" region.

It corrects malformed international numbers that start with a 0 instead of a +.

It correctly identifies 4-digit internal extensions and categorizes them as "Internal".