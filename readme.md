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

## 📈 Monitoring & Troubleshooting

### Current System Status
- **Active Customers**: Meydan, SPC, Shams (ETL running successfully every 5 minutes via PM2)
- **Pending Customer**: DubaiSouth (authentication issues - requires credential verification)
- **Schedule**: Every 5 minutes via PM2 cron (production Linux environment)
- **Data Volume**: Incremental loads with detailed logging
- **Database**: Local MySQL with separate databases per customer
- **Monitoring**: Comprehensive logs showing record counts and processing details
- **Status**: ✅ Production deployment successful

### Log Files
- PM2 logs: `/home/multycomm/allcdrpipeline/logs/etl.log` (detailed ETL execution with record counts)
- Application logs: Console output with timestamps
- Database errors: Check MySQL error logs

### Log Details Captured
- ✅ API authentication success/failure
- ✅ Records fetched, inserted, and duplicates skipped
- ✅ Processing batch completion (X/Y records)
- ✅ Pagination handling (multiple API pages)
- ✅ Phase completion status
- ❌ Authentication errors and failures

### Common Issues
- **Auth Failures**: Verify credentials and endpoints
- **Connection Errors**: Check network and database status
- **Duplicate Data**: Normal for overlapping time windows
- **Memory Issues**: PM2 auto-restarts on high usage
- **API Date Range Limit**: The CDR API only accepts date ranges up to 30 days. Set `INITIAL_LOAD_DAYS=30` maximum.
- **PM2 Path Issues**: Update `ecosystem.config.js` with correct Linux paths in production (not Windows paths)

### Performance Tuning
- Adjust `FETCH_INTERVAL_MINUTES` for data volume
- Modify PM2 cron schedule based on business needs
- Monitor database connection pool usage

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
