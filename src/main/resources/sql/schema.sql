
CREATE TABLE etl_proceso (
    proceso_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fecha_inicio TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    fecha_termino TIMESTAMP,
    total_leidos INTEGER NOT NULL DEFAULT 0,
    total_validos INTEGER NOT NULL DEFAULT 0,
    total_rechazados INTEGER NOT NULL DEFAULT 0,
    estado VARCHAR(20) NOT NULL DEFAULT 'EN_PROCESO'
        CHECK (estado IN ('EN_PROCESO', 'COMPLETADO', 'ERROR')),
    mensaje_error TEXT
);

CREATE TABLE staging_clientes (
    staging_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    proceso_id BIGINT NOT NULL REFERENCES etl_proceso(proceso_id),
    linea_original TEXT NOT NULL,
    cliente_id BIGINT,
    nombre VARCHAR(200),
    email VARCHAR(320),
    estado VARCHAR(20) NOT NULL DEFAULT 'PENDIENTE'
        CHECK (estado IN ('PENDIENTE', 'VALIDO', 'RECHAZADO')),
    motivo_rechazo TEXT,
    fecha_carga TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clientes (
    cliente_id BIGINT PRIMARY KEY,
    nombre VARCHAR(200) NOT NULL,
    email VARCHAR(320) NOT NULL,
    fecha_creacion TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE clientes_rechazados (
    rechazo_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    proceso_id BIGINT NOT NULL REFERENCES etl_proceso(proceso_id),
    linea_original TEXT NOT NULL,
    motivo TEXT NOT NULL,
    fecha_rechazo TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
);

CREATE INDEX idx_staging_clientes_proceso
    ON staging_clientes(proceso_id);

CREATE INDEX idx_rechazados_proceso
    ON clientes_rechazados(proceso_id);
