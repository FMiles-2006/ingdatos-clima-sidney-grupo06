-- Crea la base de datos de serving (capa gold) separada de la metadata de Airflow.
-- Se ejecuta automáticamente en el primer arranque de postgres.
SELECT 'CREATE DATABASE gold'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'gold')\gexec
