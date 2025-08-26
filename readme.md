Python ETL Pipeline for CDR API to MySQL Data Warehouse
📝 Objective
This project provides a robust, two-phase ETL (Extract, Transform, Load) pipeline in Python. It is designed to fetch Call Detail Records (CDRs) from a REST API, load the raw data into a staging table, and then transform it into a structured star schema within a MySQL database, making it ready for analytics and business intelligence.

The pipeline is designed to be executed on a recurring schedule (e.g., hourly) and handles pagination, duplicate records, and data transformation automatically.

📂 Code Structure
The project is organized into the following files:

run_pipeline.py: (Main Script) The master script used to execute the entire ETL pipeline in the correct sequence.

cdr_ingestion.py: The script for Phase 1 (Extract), responsible for fetching raw data from the API and loading it into a staging table.

etl_phase2.py: The script for Phase 2 (Transform & Load), which processes the raw data and populates the final star schema.

requirements.txt: A list of all the Python libraries required to run the pipeline.

.env: A configuration file for storing all secrets and settings (API keys, database credentials).

schema.sql: The SQL script to create the initial raw data staging table.

schema_phase2.sql: The complete SQL script to create all final dimension and fact tables for the data warehouse.

🚀 Getting Started
Follow these steps to set up and run the ETL pipeline.

Prerequisites
Python 3.8 or newer.

Access to a MySQL server (local or remote).

MySQL client tools (e.g., MySQL Workbench, mysql CLI).

1. Database and User Setup
Before running the scripts, you need to prepare the database.

Connect to your MySQL server as an administrator (root).

Create the database:

SQL

CREATE DATABASE your_db_name;
Create a dedicated user for the script to enhance security:

SQL

CREATE USER 'your_db_user'@'localhost' IDENTIFIED BY 'your_db_password';
GRANT ALL PRIVILEGES ON your_db_name.* TO 'your_db_user'@'localhost';
FLUSH PRIVILEGES;
Run the contents of the schema_phase2.sql file to create all the necessary tables (cdr_raw_data, fact_calls, and all dim_ tables) inside your new database. Make sure to add USE your_db_name; at the top of the script before running it.

Duplicate Handling
The pipeline is resilient against duplicate data. This is achieved at multiple levels:

The cdr_raw_data staging table has a UNIQUE index on the msg_id column.

The Phase 1 script (cdr_ingestion.py) uses an INSERT IGNORE command, which instructs MySQL to gracefully skip any record whose msg_id already exists.

The final fact_calls table also has a UNIQUE index on msg_id to provide a final layer of data integrity.

2. Installation
Clone the repository or download the project files.

Navigate to the project's root directory in your terminal.

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

Open the new .env file and fill in your credentials:

Ini, TOML

# .env

# API Credentials
API_JWT_TOKEN="your_jwt_token_here"
API_ACCOUNT_ID="your_account_id_here"

# Scheduling Configuration
FETCH_INTERVAL_MINUTES=60

# MySQL Database Credentials
DB_HOST="localhost"
DB_USER="your_db_user"
DB_PASSWORD="your_db_password"
DB_NAME="your_db_name"
▶️ How to Run the Pipeline
To run the entire ETL process, execute the main pipeline script. This will run Phase 1 first, followed immediately by Phase 2.

Make sure your virtual environment is activated.

Execute the run_pipeline.py script from your terminal:

Bash

python run_pipeline.py
The script will print detailed log messages to the console, showing the progress of both phases, the number of records ingested and transformed, and any warnings or errors that occur.

⚙️ Scheduling the Pipeline
For automation, you should schedule the run_pipeline.py script to run at your desired interval (e.g., every hour).

Scheduling with Cron (Linux/macOS)
Open your crontab file for editing: crontab -e

Add the following line, making sure to use the absolute paths to your project and virtual environment. This example runs the script at the top of every hour.

Code snippet

# Run the full CDR ETL pipeline every hour
0 * * * * /path/to/your/project/venv/bin/python /path/to/your/project/run_pipeline.py
Scheduling with Windows Task Scheduler
Open Task Scheduler.

Click Create Basic Task... in the "Actions" pane.

Name: Give it a clear name like "Hourly CDR ETL Pipeline".

Trigger: Select "Daily" and set it to repeat every 1 hour.

Action: Select "Start a program".

Program/script: Provide the full path to the python.exe executable inside your virtual environment (e.g., C:\path\to\your\project\venv\Scripts\python.exe).

Add arguments (optional): Enter the name of the master script: run_pipeline.py.

Start in (optional): Provide the full path to your project's root folder (e.g., C:\path\to\your\project).

Review the settings and click Finish.