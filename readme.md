Python Script for CDR API Data Ingestion to MySQL
📝 Objective
This project provides a robust Python script designed to fetch Call Detail Records (CDRs) from a REST API and ingest the raw data into a MySQL database. It serves as the first phase of a data pipeline, designed to be executed on an hourly schedule.

The script handles API authentication, automatically paginates through results to fetch all records within a given time frame, and stores the complete JSON object for each record in a staging table for later processing.

📂 Code Structure
The project is organized into four distinct files:

cdr_ingestion.py: The main Python script that contains all the logic for fetching, processing, and storing the data.

requirements.txt: A list of all the Python libraries required to run the script.

.env: The configuration file where all sensitive information (API keys, database credentials) is stored. This file should never be committed to version control. An example is provided in .env.example.

schema.sql: The SQL script containing the CREATE TABLE statement needed to set up the initial staging table in your MySQL database.

🚀 Getting Started
Follow these steps to set up and run the ingestion script.

## Prerequisites
Python 3.8 or newer installed.

Access to a MySQL server (local or remote).

MySQL client tools for your operating system (e.g., MySQL Workbench, mysql command-line client).

## 1. Database and User Setup
Before running the script, you need to prepare the database.

Using a MySQL client, connect to your server as an administrator (root).

Create the database that will store the data:

SQL

CREATE DATABASE your_db_name;
Create a dedicated user for the script to enhance security. Replace the placeholder values with your actual credentials.

SQL

CREATE USER 'your_db_user'@'localhost' IDENTIFIED BY 'your_db_password';
GRANT ALL PRIVILEGES ON your_db_name.* TO 'your_db_user'@'localhost';
FLUSH PRIVILEGES;
Run the contents of the schema.sql file to create the cdr_raw_data table inside your new database. Make sure to add USE your_db_name; at the top of the script.

### ## Duplicate Handling

The script is designed to be resilient against duplicate data. This is achieved through a database-level uniqueness constraint.

-   The `cdr_raw_data` table has a `UNIQUE` index on the `msg_id` column.
-   The Python script extracts the `msg_id` from each record fetched from the API.
-   It uses an `INSERT IGNORE` command, which instructs the MySQL database to gracefully skip inserting any record whose `msg_id` already exists in the table.

This ensures that even if the API provides overlapping data on consecutive runs, no duplicate records will be created. The script's log output will show how many records were fetched versus how many were newly inserted.

## 2. Installation
Clone the repository or download the project files to a folder on your machine.

Open a terminal or command prompt and navigate to the project's root directory.

Bash

cd path/to/your/project
Create a Python virtual environment. This isolates the project's dependencies.

Bash

python3 -m venv venv
Activate the virtual environment.

On Windows:

DOS

.\venv\Scripts\activate
On Linux/macOS:

Bash

source venv/bin/activate
Install the required libraries using pip.

Bash

pip install -r requirements.txt
## 3. Configuration
Find the .env.example file in the project directory.

Create a copy of this file and rename it to .env.

Open the new .env file and fill in your specific credentials:

Ini, TOML

# API Credentials
API_JWT_TOKEN="your_jwt_token_here"
API_ACCOUNT_ID="your_account_id_here"

# Scheduling Configuration
FETCH_INTERVAL_MINUTES=60

# MySQL Database Credentials
DB_HOST="localhost"
DB_USER="your_db_user" # The user you created in Step 1
DB_PASSWORD="your_db_password" # The password for that user
DB_NAME="your_db_name" # The database you created in Step 1
▶️ How to Run the Script
You can run the script manually for testing or on-demand data pulls.

Make sure your virtual environment is activated.

Execute the Python script from your terminal:

Bash

python cdr_ingestion.py
The script will print log messages to the console, showing its progress, the final URL being used, the number of records ingested, and any errors that occur.

⚙️ Scheduling the Script
## Scheduling with Cron (Linux/Ubuntu)
Cron is the standard job scheduler on Linux systems.

Open your user's crontab file for editing:

Bash

crontab -e
Add the following line to the bottom of the file. This schedule will run the script at the top of every hour. Make sure to replace /path/to/your/project/ with the absolute path to your project folder.

Code snippet

# Run the CDR ingestion script every hour
0 * * * * /path/to/your/project/venv/bin/python /path/to/your/project/cdr_ingestion.py
Breakdown of the cron command:

0 * * * *: This is the schedule. It means "at minute 0, every hour, every day of the month, every month, every day of the week."

/path/to/your/project/venv/bin/python: This is the absolute path to the Python executable inside your virtual environment. This is crucial for ensuring the script uses the correct libraries.

/path/to/your/project/cdr_ingestion.py: The absolute path to the script you want to run.

Save and close the file. Cron will automatically apply the new schedule.

## Scheduling with Windows Task Scheduler
Open Task Scheduler on your Windows machine.

In the right-hand "Actions" pane, click "Create Basic Task...".

Name and Description: Give the task a clear name (e.g., "Hourly CDR Ingestion").

Trigger: Select "Daily", then set it to recur every 1 day. In the next screen, specify that it should repeat the task every 1 hour for a duration of "Indefinitely".

Action: Select "Start a program".

Program/script: This is the most important step.

Program/script: Provide the full path to the python.exe executable INSIDE your virtual environment. (e.g., C:\path\to\your\project\venv\Scripts\python.exe)

Add arguments (optional): Enter the name of your script (cdr_ingestion.py).

Start in (optional): Provide the full path to your project's root folder (e.g., C:\path\to\your\project).

Finish: Review the settings and click Finish. The task is now scheduled.