module.exports = {
  apps: [{
    name: 'allcdr-etl-scheduler',
    script: 'run_all_customers.py',
    interpreter: 'python3',  // Use system python3 in production
    cwd: '/home/multycomm/allcdrpipeline/ALLCDRTOSQL1',  // Linux production path
    env: {
      NODE_ENV: 'production'
    },
    cron_restart: '*/5 * * * *',  // Run every 5 minutes
    autorestart: true,
    // restart_delay: 300000,  // Restart after 5 minutes (300 seconds)
    max_memory_restart: '1G',
    log_file: '/home/multycomm/allcdrpipeline/logs/etl.log',  // Linux log path
    out_file: '/home/multycomm/allcdrpipeline/logs/etl-out.log',
    error_file: '/home/multycomm/allcdrpipeline/logs/etl-error.log',
    time: true
  }]
};