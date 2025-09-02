-- This script runs automatically the first time the MySQL container is started.

-- Create the database for the 'shams' customer
CREATE DATABASE IF NOT EXISTS `allcdr_shams`;

-- Create the database for the 'spc' customer
CREATE DATABASE IF NOT EXISTS `allcdr_spc`;

-- Create the database for the 'dubaiSouth' customer
CREATE DATABASE IF NOT EXISTS `allcdr_dubaisouth`;

-- Grant all necessary privileges to the user defined in the .env file
-- This allows the user to connect to and manage both customer databases.
GRANT ALL PRIVILEGES ON `allcdr_shams`.* TO '${DB_USER}'@'%';
GRANT ALL PRIVILEGES ON `allcdr_spc`.* TO '${DB_USER}'@'%';
GRANT ALL PRIVILEGES ON `allcdr_dubaisouth`.* TO '${DB_USER}'@'%';

-- Apply the new privileges
FLUSH PRIVILEGES;