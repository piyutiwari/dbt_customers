-- Database Setup Script for dbt PostgreSQL Medallion Project
-- Run this script as a PostgreSQL superuser to set up the database

-- Create database
CREATE DATABASE medallion_db;

-- Connect to the database
\c medallion_db

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS snapshots;

-- Grant privileges to the user (replace 'postgres' with your username if different)
GRANT ALL PRIVILEGES ON DATABASE medallion_db TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA bronze TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA silver TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA gold TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA snapshots TO postgres;
GRANT ALL PRIVILEGES ON SCHEMA public TO postgres;

-- Display confirmation
SELECT 'Database and schemas created successfully!' AS status;
