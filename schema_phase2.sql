-- schema_final.sql
-- This is a complete setup script for the entire CDR database.
-- It creates the raw data table AND all star schema tables.

-- Ensure you have selected your database first!
-- USE your_db_name;

-- -----------------------------------------------------
-- 1. Raw Data Staging Table
-- -----------------------------------------------------
CREATE TABLE cdr_raw_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    msg_id VARCHAR(255) UNIQUE,
    record_data JSON,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_processed_at TIMESTAMP NULL -- Column to track ETL status
);

-- -----------------------------------------------------
-- 2. Dimension Tables
-- -----------------------------------------------------

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    `year` INT NOT NULL,
    `quarter` TINYINT NOT NULL,
    `month` TINYINT NOT NULL,
    day_of_week VARCHAR(10) NOT NULL,
    INDEX (full_date)
);

CREATE TABLE dim_time_of_day (
    time_key INT PRIMARY KEY,
    full_time TIME NOT NULL,
    `hour` TINYINT NOT NULL,
    `minute` TINYINT NOT NULL,
    INDEX (full_time)
);

CREATE TABLE dim_users (
    user_key INT AUTO_INCREMENT PRIMARY KEY,
    user_number VARCHAR(255) UNIQUE NOT NULL,
    user_name VARCHAR(255),
    country_code VARCHAR(10),
    country_name VARCHAR(100),
    is_agent BOOLEAN DEFAULT FALSE
);

CREATE TABLE dim_call_disposition (
    disposition_key INT AUTO_INCREMENT PRIMARY KEY,
    call_direction VARCHAR(50),
    hangup_cause VARCHAR(100),
    disposition VARCHAR(100),
    subdisposition VARCHAR(100),
    UNIQUE KEY unique_disposition (call_direction, hangup_cause, disposition, subdisposition)
);

CREATE TABLE dim_system (
    system_key INT AUTO_INCREMENT PRIMARY KEY,
    switch_hostname VARCHAR(255) UNIQUE NOT NULL,
    app_name VARCHAR(100),
    realm VARCHAR(255)
);

CREATE TABLE dim_campaigns (
    campaign_key INT AUTO_INCREMENT PRIMARY KEY,
    campaign_id VARCHAR(255) UNIQUE NOT NULL,
    campaign_name VARCHAR(255)
);

CREATE TABLE dim_queues (
    queue_key INT AUTO_INCREMENT PRIMARY KEY,
    queue_id VARCHAR(255) UNIQUE NOT NULL,
    queue_name VARCHAR(255)
);

-- -----------------------------------------------------
-- 3. Fact Tables
-- -----------------------------------------------------

CREATE TABLE fact_calls (
    call_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    msg_id VARCHAR(255) UNIQUE NOT NULL,
    call_id VARCHAR(255),
    date_key INT,
    time_key INT,
    caller_user_key INT,
    callee_user_key INT,
    disposition_key INT,
    system_key INT,
    campaign_key INT,
    queue_key INT,
    duration_seconds INT,
    billing_seconds INT,
    call_recording_url VARCHAR(512),
    is_conference BOOLEAN,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (time_key) REFERENCES dim_time_of_day(time_key),
    FOREIGN KEY (caller_user_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (callee_user_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (disposition_key) REFERENCES dim_call_disposition(disposition_key),
    FOREIGN KEY (system_key) REFERENCES dim_system(system_key),
    FOREIGN KEY (campaign_key) REFERENCES dim_campaigns(campaign_key),
    FOREIGN KEY (queue_key) REFERENCES dim_queues(queue_key)
);

CREATE TABLE fact_agent_legs (
    agent_leg_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    call_key BIGINT,
    agent_key INT,
    disposition_key INT,
    wait_seconds INT,
    talk_seconds INT,
    wrap_up_seconds INT,
    FOREIGN KEY (call_key) REFERENCES fact_calls(call_key),
    FOREIGN KEY (agent_key) REFERENCES dim_users(user_key),
    FOREIGN KEY (disposition_key) REFERENCES dim_call_disposition(disposition_key)
);


/* -- This script updates the schema to support nested subdispositions and follow-up notes.
-- Run these commands on your 'allcdr' database.
*/

-- 1. Add a column to the fact table for the notes
ALTER TABLE fact_calls
ADD COLUMN follow_up_notes TEXT;

-- 2. Rename the old subdisposition column to be the first level
ALTER TABLE dim_call_disposition
CHANGE COLUMN subdisposition subdisposition_1 VARCHAR(100);

-- 3. Add a new column for the second level of subdisposition
ALTER TABLE dim_call_disposition
ADD COLUMN subdisposition_2 VARCHAR(100);

-- 4. Update the unique key to include the new columns to maintain integrity
ALTER TABLE dim_call_disposition
DROP INDEX unique_disposition,
ADD UNIQUE KEY unique_disposition (call_direction, hangup_cause, disposition, subdisposition_1, subdisposition_2);