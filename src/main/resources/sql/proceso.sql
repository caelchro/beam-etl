-- Iniciar un proceso ETL
INSERT INTO etl_proceso DEFAULT VALUES
RETURNING proceso_id;