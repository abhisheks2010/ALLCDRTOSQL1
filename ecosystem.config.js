module.exports = {
  apps: [{
    name: 'allcdr-etl-scheduler',
    script: 'run_all_customers.py',
    interpreter: 'C:/Users/abhis/OneDrive/Documents/MULTYCOMM/ALLCDRTOSQL/venv/Scripts/python.exe',  // Update to your Python venv path
    cwd: 'C:/Users/abhis/OneDrive/Documents/MULTYCOMM/ALLCDRTOSQL',  // Update to your project path
    env: {
      NODE_ENV: 'production'
    },
    cron_restart: '*/5 * * * *',  // Run every 5 minutes
    autorestart: true,
    // restart_delay: 300000,  // Restart after 5 minutes (300 seconds)
    max_memory_restart: '1G',
    log_file: 'C:/Users/abhis/OneDrive/Documents/MULTYCOMM/logs/etl.log',  // Update log path
    out_file: 'C:/Users/abhis/OneDrive/Documents/MULTYCOMM/logs/etl-out.log',
    error_file: 'C:/Users/abhis/OneDrive/Documents/MULTYCOMM/logs/etl-error.log',
    time: true
  }]
};