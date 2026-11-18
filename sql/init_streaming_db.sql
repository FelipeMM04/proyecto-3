-- 1. TABLAS BASE DEL DATA WAREHOUSE 

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    codigo_municipio INT PRIMARY KEY,
    municipio VARCHAR(150) NOT NULL,
    codigo_departamento INT NOT NULL,
    departamento VARCHAR(150) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_categoria (
    id_categoria INT PRIMARY KEY,
    categoria VARCHAR(150) NOT NULL,
    sub_categoria VARCHAR(150) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_turismo_anual (
    id_fact SERIAL PRIMARY KEY,
    codigo_rnt INT NOT NULL,
    codigo_municipio INT REFERENCES dim_ubicacion(codigo_municipio),
    id_categoria INT REFERENCES dim_categoria(id_categoria),
    anio INT NOT NULL,
    numero_de_habitaciones INT NOT NULL DEFAULT 0,
    numero_de_camas INT NOT NULL DEFAULT 0,
    numero_de_empleados INT NOT NULL DEFAULT 0
);

-- ============================================================================
-- 2. STREAMING METRICS STORE (Tabla destino para el Consumer de Kafka)
-- ============================================================================
-- Almacena métricas agregadas en tiempo real por departamento:
-- Métrica 1: total_empleados_stream (Empleo formal directo acumulado)
-- Métrica 2: total_habitaciones_stream y total_camas_stream (Capacidad instalada)
-- Métrica 3: total_pst_procesados (Prestadores procesados)
-- Métrica 4: alertas_alto_impacto_laboral (PSTs con >= 20 empleados directos)

CREATE TABLE IF NOT EXISTS streaming_metrics (
    departamento VARCHAR(150) PRIMARY KEY,
    total_pst_procesados INT NOT NULL DEFAULT 0,
    total_empleados_stream INT NOT NULL DEFAULT 0,
    total_habitaciones_stream INT NOT NULL DEFAULT 0,
    total_camas_stream INT NOT NULL DEFAULT 0,
    alertas_alto_impacto_laboral INT NOT NULL DEFAULT 0,
    ultimo_event_id VARCHAR(100),
    ultima_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla para registrar evidencia de eventos rechazados por romper el contrato
CREATE TABLE IF NOT EXISTS streaming_invalid_events (
    id_error SERIAL PRIMARY KEY,
    raw_payload TEXT,
    motivo_rechazo VARCHAR(255),
    fecha_rechazo TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 3. DATOS DE MUESTRA DEL RNT (Para simular lectura desde fact_turismo_anual)
-- ============================================================================

INSERT INTO dim_ubicacion (codigo_municipio, municipio, codigo_departamento, departamento) VALUES
(76001, 'CALI', 76, 'VALLE DEL CAUCA'),
(13001, 'CARTAGENA', 13, 'BOLIVAR'),
(11001, 'BOGOTA, D.C.', 11, 'BOGOTA, D.C.'),
(5001, 'MEDELLIN', 5, 'ANTIOQUIA'),
(47001, 'SANTA MARTA', 47, 'MAGDALENA'),
(63001, 'ARMENIA', 63, 'QUINDIO')
ON CONFLICT (codigo_municipio) DO NOTHING;

INSERT INTO dim_categoria (id_categoria, categoria, sub_categoria) VALUES
(1, 'ESTABLECIMIENTOS DE ALOJAMIENTO Y HOSPEDAJE', 'HOTEL'),
(2, 'ESTABLECIMIENTOS DE ALOJAMIENTO Y HOSPEDAJE', 'HOSTAL'),
(3, 'AGENCIAS DE VIAJES', 'AGENCIA DE VIAJES Y DE TURISMO'),
(4, 'VIVIENDAS TURISTICAS', 'APARTAMENTO TURISTICO'),
(5, 'ESTABLECIMIENTOS DE GASTRONOMIA Y SIMILARES', 'RESTAURANTE TURISTICO')
ON CONFLICT (id_categoria) DO NOTHING;

INSERT INTO fact_turismo_anual (codigo_rnt, codigo_municipio, id_categoria, anio, numero_de_habitaciones, numero_de_camas, numero_de_empleados) VALUES
(10245, 76001, 1, 2025, 45, 80, 28),
(10890, 76001, 3, 2025, 0, 0, 12),
(20411, 13001, 1, 2025, 120, 240, 85),
(20988, 13001, 4, 2025, 15, 30, 6),
(30112, 11001, 1, 2025, 95, 160, 64),
(30544, 11001, 5, 2025, 0, 0, 32),
(40210, 5001, 1, 2025, 70, 130, 41),
(40789, 5001, 2, 2025, 18, 42, 9),
(50123, 47001, 1, 2025, 110, 225, 73),
(50677, 47001, 4, 2025, 10, 22, 4),
(60321, 63001, 1, 2025, 35, 72, 21),
(60899, 63001, 2, 2025, 14, 28, 7),
(70155, 76001, 1, 2026, 60, 115, 35),
(80244, 13001, 1, 2026, 150, 310, 110),
(90312, 5001, 1, 2026, 85, 155, 52);