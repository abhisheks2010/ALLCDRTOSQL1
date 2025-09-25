module.exports = {
  apps: [{
    name: 'allcdr-etl-scheduler',
    script: 'run_all_customers.py',
    interpreter: 'python3',  // Use system python3 in production
    cwd: '/home/multycomm/allcdrpipeline/ALLCDRTOSQL1',  // Linux production path
    env: {
      NODE_ENV: 'production'
    },
    // Remove cron_restart and let the script handle its own scheduling
    autorestart: true,
    restart_delay: 5000,  // 5 second delay before restart if process crashes
    max_memory_restart: '1G',
    log_file: '/home/multycomm/allcdrpipeline/logs/etl.log',  // Linux log path
    out_file: '/home/multycomm/allcdrpipeline/logs/etl-out.log',
    error_file: '/home/multycomm/allcdrpipeline/logs/etl-error.log',
    time: true
  }]
};