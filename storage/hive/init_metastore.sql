-- Cria banco dedicado para o Hive Metastore
-- Rodado pelo mesmo Postgres que serve o data source
SELECT 'CREATE DATABASE hive_metastore'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'hive_metastore')\gexec

GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO admin;
