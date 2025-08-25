CREATE DATABASE allcdr;
USE DATABASE allcdr;

-- schema.sql
-- This script creates the staging table for raw CDR data.

CREATE TABLE cdr_raw_data (
    -- A unique identifier for each row in this table.
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- The complete JSON object for a single call record from the API.
    record_data JSON,
    
    -- The timestamp when the record was inserted into this table.
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Note: If your MySQL version is older than 5.7.8, it might not support the JSON data type.
-- In that case, you can use LONGTEXT as a replacement:
-- record_data LONGTEXT,


-- schema.sql
-- This script creates the staging table with a uniqueness constraint.

-- If the table already exists, run this command first to add the unique key column.
-- ALTER TABLE cdr_raw_data ADD COLUMN msg_id VARCHAR(255) UNIQUE;

CREATE TABLE cdr_raw_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- A unique identifier from the API record to prevent duplicates.
    msg_id VARCHAR(255) UNIQUE,

    -- The complete JSON object for a single call record from the API.
    record_data JSON,
    
    -- The timestamp when the record was inserted into this table.
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);