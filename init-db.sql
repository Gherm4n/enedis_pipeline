-- This script runs once when the Postgres container first boots.
-- It creates the data warehouse database alongside the airflow_db.

SELECT 'CREATE DATABASE data_warehouse'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'data_warehouse'
)\gexec
