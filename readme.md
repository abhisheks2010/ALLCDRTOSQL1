Of course. Here is the final, comprehensive readme.md file.

This version includes all the latest updates, including the multi-customer architecture, Dockerized database setup, and advanced data handling logic. It serves as a complete user manual for the project.

readme.md
Python ETL Pipeline for CDR API to MySQL Data Warehouse
📝 Objective
This project provides a robust, multi-tenant, two-phase ETL (Extract, Transform, Load) pipeline in Python. It is designed to fetch Call Detail Records (CDRs) from multiple customer REST APIs, load the raw data into a dedicated staging table, and then transform it into a structured star schema within a MySQL database, making it ready for analytics and business intelligence.

The entire environment, including the database, is containerized with Docker for easy setup and consistent deployment.

📂 Code Structure
The project is organized into the following files:

run_pipeline.py: (Main Script) The master script used to execute the entire ETL pipeline for a specific customer.

docker-compose.yml: Defines and configures the MySQL database service.

cdr_ingestion.py: The script for Phase 1 (Extract), responsible for fetching raw data from the API.

etl_phase2.py: The script for Phase 2 (Transform & Load), which populates the final star schema.

requirements.txt: A list of all the Python libraries required.

.env: A configuration file for storing all secrets and settings for all customers.

init-scripts/: A folder containing SQL scripts that are run once to initialize the databases.

schema_phase2.sql: The complete SQL script to create all final dimension and fact tables.

🚀 Getting Started
Follow these steps to set up and run the entire environment.

Prerequisites
Docker and Docker Compose must be installed and running.

Python 3.8 or newer.

1. Configuration
Clone the repository and switch to the correct branch:

Bash

git clone https://github.com/abhisheks2010/ALLCDRTOSQL1.git
cd ALLCDRTOSQL1
git checkout multiCustomer
Create a copy of the .env.example file and rename it to .env.

Open the new .env file and fill in the credentials for all your customers, using a unique prefix for each one. Crucially, set DB_HOST to the service name from docker-compose.yml (mysql_db).

Ini, TOML

# .env - Multi-Customer Configuration

# --- Customer 1: SHAMS Configuration ---
SHAMS_API_BASE_URL="..."
SHAMS_API_JWT_TOKEN="..."
SHAMS_API_ACCOUNT_ID="..."
SHAMS_DB_NAME="allcdr_shams"

# --- Customer 2: SPC Configuration ---
SPC_API_BASE_URL="..."
SPC_API_JWT_TOKEN="..."
SPC_API_ACCOUNT_ID="..."
SPC_DB_NAME="allcdr_spc"

# --- Common & Docker Configuration ---
FETCH_INTERVAL_MINUTES=5
DB_HOST="mysql_db"  # <-- IMPORTANT: Use the Docker service name
DB_USER="your_db_user"
DB_PASSWORD="your_strong_password"
2. Database Setup with Docker
Start the Database Container: From the project's root directory, run:

Bash

docker-compose up -d
The first time you run this, Docker will download MySQL, create the databases (allcdr_shams, allcdr_spc), and set up the user as defined in init-scripts/01-init.sql.

Create the Tables (One-Time Step): After the container is running, connect to the database using any SQL client (e.g., MySQL Workbench, DBeaver) with the credentials from your .env file (host: localhost, port: 3306).

For each customer database, run the entire schema_phase2.sql script to create the tables. Remember to set the correct USE statement at the top of the script for each run (e.g., USE allcdr_shams;).

3. Application Setup
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
▶️ How to Run the Pipeline
To run the entire ETL process, you must now provide the customer's name as a command-line argument. This name should match the prefix used in the .env file (case-insensitive).

Bash

# To run the pipeline for the SHAMS customer
python run_pipeline.py shams

# To run the pipeline for the SPC customer
python run_pipeline.py spc
⚙️ Scheduling the Pipeline
For automation, you must create a separate scheduled task for each customer.

Cron (Linux/macOS) Example:
This example runs the pipeline every 5 minutes for both customers.

Code snippet

*/5 * * * * /path/to/project/venv/bin/python /path/to/project/run_pipeline.py shams
*/5 * * * * /path/to/project/venv/bin/python /path/to/project/run_pipeline.py spc
Windows Task Scheduler Example:
Create two separate tasks. The Program/script and Start in fields will be the same, but the Add arguments (optional) field will be different:

SHAMS Task Arguments: run_pipeline.py shams

SPC Task Arguments: run_pipeline.py spc

Data Handling and Business Logic
Duplicate Handling: UNIQUE keys on msg_id and INSERT IGNORE commands prevent duplicate records.

Nested JSON Parsing: The ETL script is designed to parse complex, nested JSON objects from the fonoUC object, including Follow-up Notes and up to two levels of Subdispositions.

Robust Phone Number Parsing: The script contains intelligent logic to handle a variety of phone number formats, including standard international, local UAE numbers, malformed numbers, and 4-digit internal extensions.