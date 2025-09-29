# Python ETL Pipeline for CDR API to MySQL Data Warehouse

## 📝 Objective
This project provides a robust, multi-tenant, two-phase ETL (Extract, Transform, Load) pipeline in Python. It fet## 👤 Author

**ABHISHEK SHARMA**  
📧 abhi.s.manc@gmail.com Call Detail Records (CDRs) from multiple customer REST APIs, loads raw data into staging tables, and transforms it into a structured star schema within MySQL databases, ready for analytics and business intelligence.

## 📂 Code Structure
- `run_pipeline.py`: Main script to execute ETL pipeline for a specific customer
- `run_all_customers.py`: Automated scheduler for all customers
- `cdr_ingestion.py`: Phase 1 (Extract) - fetches raw data from APIs
- `etl_phase2.py`: Phase 2 (Transform & Load) - populates star schema
- `requirements.txt`: Python dependencies
- `.env`: Configuration for all customers and settings
- `schema_phase2.sql`: SQL script for dimension and fact tables
- `ecosystem.config.js`: PM2 configuration for production deployment
- `init-scripts/`: SQL initialization scripts

## 🚀 Getting Started

### Prerequisites
- **OS**: Debian Linux (production deployment)
- Python 3.8 or newer
- MySQL 8.0 or newer (local installation)
- Node.js and npm (for PM2 deployment)

### 1. Environment Setup
```bash
# Clone repository
git clone https://github.com/abhisheks2010/ALLCDRTOSQL1.git
cd ALLCDRTOSQL1
git checkout dockerizeDB

# Create virtual environment (development)
python -m venv venv
venv\Scripts\activate  # Windows

# For production Debian deployment, ensure python3 is available
# and all required packages are installed system-wide or in venv
# Create logs directory
mkdir -p /home/multycomm/allcdrpipeline/logs

# Install dependencies
pip install -r requirements.txt
```

### 2. Database Setup
```bash
# Install MySQL locally and create databases
mysql -u root -p
CREATE DATABASE allcdr_meydan;
CREATE DATABASE allcdr_spc;
CREATE DATABASE allcdr_shams;
CREATE DATABASE allcdr_dubaisouth;
exit

# Run schema for each database
mysql -u root -p allcdr_meydan < schema_phase2.sql
mysql -u root -p allcdr_spc < schema_phase2.sql
mysql -u root -p allcdr_shams < schema_phase2.sql
mysql -u root -p allcdr_dubaisouth < schema_phase2.sql
```

### 3. Configuration
Copy `.env` and configure credentials for each customer:
```bash
cp .env.example .env
# Edit .env with your API credentials and database settings
```

### 4. Initial Load (One-time)
```bash
# Set initial load days in .env (max 30 days due to API limitations)
INITIAL_LOAD_DAYS=30

# Run for each customer
python run_pipeline.py meydan
python run_pipeline.py spc
python run_pipeline.py shams
# dubaisouth may fail due to auth issues

# Comment out INITIAL_LOAD_DAYS after initial load
```

## 🔄 Automated Scheduling with PM2

### Install PM2
```bash
npm install -g pm2
```

### Start Production Scheduler
```bash
# Create logs directory if it doesn't exist
mkdir -p /home/multycomm/allcdrpipeline/logs

# Update ecosystem.config.js with correct production paths before starting
# Edit interpreter, cwd, and log paths for your Linux environment
pm2 start ecosystem.config.js

# Check status
pm2 status

# View real-time logs
pm2 logs allcdr-etl-scheduler

# Stop scheduler
pm2 stop allcdr-etl-scheduler

# Restart scheduler
pm2 restart allcdr-etl-scheduler
```

### How It Works
- PM2 starts the `run_all_customers.py` script and keeps it running continuously
- The script runs in an infinite loop, executing ETL for all customers every 5 minutes
- Each complete cycle takes ~15-20 seconds, then sleeps for 5 minutes
- PM2 monitors the process and restarts it if it crashes

### PM2 Configuration
- **Process Management**: PM2 keeps the ETL scheduler running continuously on Debian Linux
- **Internal Scheduling**: Script runs ETL cycles every 5 minutes internally
- **Sequential Processing**: Processes all customers with 5-second delays between them
- **Memory Limit**: 1GB with auto-restart
- **Logging**: Detailed logs with record counts and processing metrics

### Manual Testing
```bash
# Test all customers manually
python run_all_customers.py

# Test single customer
python run_pipeline.py meydan

# Monitor PM2 logs
pm2 logs allcdr-etl-scheduler
```

## ⚙️ Automated Scheduling: PM2 & run_all_customers.py

### PM2 Process Manager
- **Configuration:** See `ecosystem.config.js` for production deployment.
- **Script:** Runs `run_all_customers.py` using system `python3`.
- **Working Directory:** Set to `/home/multycomm/allcdrpipeline/ALLCDRTOSQL1` (Linux production path).
- **Autorestart:** Enabled; process will restart automatically on crash.
- **Memory Limit:** 1GB (`max_memory_restart: '1G'`).
- **Logging:**
  - All logs written to `/home/multycomm/allcdrpipeline/logs/etl.log` (and out/error logs).
  - Timestamps and process info included (`time: true`).
- **No External Cron:** The script handles its own 5-minute scheduling loop; PM2 only manages process lifecycle.

### `run_all_customers.py` Scheduler Logic
- **Customer List:** Edit the `CUSTOMERS` list in the script to control which tenants are processed.
- **Loop:**
  - For each customer, runs `run_pipeline.py <customer>` as a subprocess.
  - Waits 5 seconds between customers to reduce DB/API load.
  - After all customers, sleeps for 5 minutes before next cycle.
- **Logging:**
  - Logs start/end of each ETL run, per-customer results, and errors.
  - Captures and logs both stdout and stderr from each ETL subprocess.
- **Error Handling:**
  - If a customer ETL fails, logs error details but continues with next customer.
  - If the main loop encounters an error, waits 60 seconds and retries.
  - Graceful shutdown on Ctrl+C (KeyboardInterrupt).

### How to Use in Production
1. Edit `CUSTOMERS` in `run_all_customers.py` to match your tenants.
2. Update `ecosystem.config.js` paths for your environment.
3. Start with `pm2 start ecosystem.config.js`.
4. Monitor with `pm2 logs allcdr-etl-scheduler`.
5. The ETL will run for all customers every 5 minutes, with robust logging and auto-restart.

## 📊 Data Architecture

### Star Schema Design
- **fact_calls**: Central fact table with call metrics
- **fact_agent_legs**: Agent-specific call segments
- **Dimensions**: date, time_of_day, users, call_disposition, system, campaigns, queues

### Data Flow
1. **Extract**: API calls fetch CDRs with JWT authentication
2. **Load**: Raw JSON stored in `cdr_raw_data` staging table
3. **Transform**: Parsed into normalized star schema
4. **Validate**: Duplicate prevention and data integrity checks

## 🔧 API Integration Details

### Authentication
- OAuth 2.0 flow with multiple endpoint fallbacks
- Automatic token refresh per customer
- Tenant/domain-based login

### Data Handling
- **Duplicate Prevention**: `msg_id` uniqueness
- **Incremental Loads**: Timestamp-based fetching
- **Error Recovery**: Failed records marked but don't stop pipeline
- **Nested JSON**: Complex fonoUC object parsing
- **API Pagination**: Automatic handling of multiple pages using `new_start_key`

### Phone Number Processing
- International format parsing (libphonenumber)
- Internal extension handling
- Country code extraction

## 🗄️ Database Schema: Raw CDR Table

The initial staging table for raw CDRs is created as follows (see `schema.sql`):

```sql
CREATE DATABASE allcdr;
USE allcdr;

CREATE TABLE cdr_raw_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    msg_id VARCHAR(255) UNIQUE,
    record_data JSON,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
- If your MySQL version is older than 5.7.8, use `LONGTEXT` instead of `JSON` for `record_data`.
- The ETL pipeline will automatically add and use the `msg_id` column for duplicate prevention.

## 📼 Call Recording URL Logic

- The ETL constructs the `call_recording_url` in the `fact_calls` table as:
  ```
  /api/recordings/<call_id>?account=<tenant>
  ```
- `<call_id>` is taken directly from the CDR JSON field `call_id`.
- `<tenant>` is the customer/account identifier from your `.env` config (e.g., `DUBAISOUTH_API_TENANT`).
- The backend Express route `/api/recordings/:id` extracts the recording ID from the URL path parameter and proxies the request to the upstream API.
- There is no direct recording URL or ID field in the CDR JSON other than `call_id`.

## 🔄 ETL Logic Summary (Latest)
- **Phase 1:** Ingests raw CDR JSON into `cdr_raw_data` with duplicate prevention using `msg_id`.
- **Phase 2:**
  - Converts the CDR `timestamp` (FILETIME ticks) to a proper date/time for `date_key` and `time_key`.
  - Constructs `call_recording_url` as described above.
  - Loads all normalized data into the star schema for analytics.

---

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Test changes thoroughly
4. Update documentation
5. Submit pull request

## � Author

**ABHISHEK SHARMA**  
📧 abhishek@multycomm.com

## �📄 License

This project is proprietary software for MultyComm.
