-- ============================================================================
-- SCRIPT DDL: MODELO DIMENSIONAL (DATA WAREHOUSE)
-- Proyecto: Pipeline Centralizado de Extraccion y Limpieza Multi-Plataforma (ETL)
-- Motor: Microsoft Azure SQL Database / SQL Server (T-SQL)
-- Arquitectura: Esquema en Estrella (Star Schema) - Metodologia Kimball
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ELIMINACION PREVENTIVA DE TABLAS (ORDEN POR DEPENDENCIAS DE FK)
-- ----------------------------------------------------------------------------
IF OBJECT_ID('dbo.HECHOS_METRICAS_PUBLICACION', 'U') IS NOT NULL 
    DROP TABLE dbo.HECHOS_METRICAS_PUBLICACION;

IF OBJECT_ID('dbo.DIM_PUBLICACION', 'U') IS NOT NULL 
    DROP TABLE dbo.DIM_PUBLICACION;

IF OBJECT_ID('dbo.DIM_CUENTA', 'U') IS NOT NULL 
    DROP TABLE dbo.DIM_CUENTA;

IF OBJECT_ID('dbo.DIM_EJECUCION', 'U') IS NOT NULL 
    DROP TABLE dbo.DIM_EJECUCION;

IF OBJECT_ID('dbo.DIM_PLATAFORMA', 'U') IS NOT NULL 
    DROP TABLE dbo.DIM_PLATAFORMA;

IF OBJECT_ID('dbo.DIM_TIEMPO', 'U') IS NOT NULL 
    DROP TABLE dbo.DIM_TIEMPO;

-- ----------------------------------------------------------------------------
-- 2. CREACION DE TABLAS DE DIMENSIONES
-- ----------------------------------------------------------------------------

-- Dimension Conforme: Tiempo
CREATE TABLE dbo.DIM_TIEMPO (
    id_tiempo INT NOT NULL,                        -- Formato numerico YYYYMMDD (ej: 20261023)
    fecha DATE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    dia INT NOT NULL,
    dia_semana INT NOT NULL,                       -- 1 (Lunes) a 7 (Domingo)
    nombre_dia VARCHAR(20) NOT NULL,
    trimestre INT NOT NULL,
    es_fin_de_semana BIT NOT NULL,
    CONSTRAINT PK_DIM_TIEMPO PRIMARY KEY CLUSTERED (id_tiempo),
    CONSTRAINT UQ_DIM_TIEMPO_FECHA UNIQUE (fecha)
);

-- Dimension Conforme: Plataforma
CREATE TABLE dbo.DIM_PLATAFORMA (
    id_plataforma VARCHAR(20) NOT NULL,            -- 'youtube', 'instagram'
    nombre_plataforma VARCHAR(50) NOT NULL,        -- 'YouTube', 'Instagram'
    version_api VARCHAR(20) NOT NULL,              -- 'v3', 'v21.0'
    tipo_fuente VARCHAR(50) NOT NULL,              -- 'Google Cloud REST API', 'Meta Graph API'
    CONSTRAINT PK_DIM_PLATAFORMA PRIMARY KEY CLUSTERED (id_plataforma)
);

-- Dimension: Cuenta (Canal de YouTube o Cuenta Instagram Business)
CREATE TABLE dbo.DIM_CUENTA (
    id_cuenta_sk INT IDENTITY(1,1) NOT NULL,       -- Clave Subrogada (Surrogate Key)
    id_cuenta_externo VARCHAR(100) NOT NULL,       -- ID original de la API
    id_plataforma VARCHAR(20) NOT NULL,
    usuario VARCHAR(100) NULL,                     -- Handle (@usuario)
    nombre VARCHAR(255) NULL,                      -- Nombre publico o titulo del canal
    fecha_hora_creacion_utc DATETIME2 NULL,
    fecha_registro_dw_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_DIM_CUENTA PRIMARY KEY CLUSTERED (id_cuenta_sk),
    CONSTRAINT FK_DIM_CUENTA_PLATAFORMA FOREIGN KEY (id_plataforma) 
        REFERENCES dbo.DIM_PLATAFORMA(id_plataforma),
    CONSTRAINT UQ_DIM_CUENTA_PLATAFORMA_EXTERNO UNIQUE (id_plataforma, id_cuenta_externo)
);

-- Dimension: Publicacion (Videos de YouTube y Posts/Reels de Instagram)
CREATE TABLE dbo.DIM_PUBLICACION (
    id_publicacion_sk INT IDENTITY(1,1) NOT NULL,  -- Clave Subrogada
    id_publicacion_externo VARCHAR(100) NOT NULL,  -- videoId o media_id
    id_plataforma VARCHAR(20) NOT NULL,
    id_cuenta_sk INT NOT NULL,
    titulo NVARCHAR(500) NULL,
    texto_descripcion NVARCHAR(MAX) NULL,
    tipo_medio VARCHAR(50) NULL,                   -- 'VIDEO', 'IMAGE', 'CAROUSEL_ALBUM'
    tipo_producto VARCHAR(50) NULL,                -- 'REELS', 'FEED', 'STORY'
    url_permalink VARCHAR(500) NULL,
    privacidad VARCHAR(50) NULL,                   -- 'public', 'unlisted', 'private'
    duracion_segundos INT NULL,
    fecha_hora_publicacion_utc DATETIME2 NULL,
    fecha_registro_dw_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_DIM_PUBLICACION PRIMARY KEY CLUSTERED (id_publicacion_sk),
    CONSTRAINT FK_DIM_PUBLICACION_PLATAFORMA FOREIGN KEY (id_plataforma) 
        REFERENCES dbo.DIM_PLATAFORMA(id_plataforma),
    CONSTRAINT FK_DIM_PUBLICACION_CUENTA FOREIGN KEY (id_cuenta_sk) 
        REFERENCES dbo.DIM_CUENTA(id_cuenta_sk),
    CONSTRAINT UQ_DIM_PUBLICACION_EXTERNO UNIQUE (id_plataforma, id_publicacion_externo)
);

-- Dimension de Auditoria y Linaje: Ejecucion del Pipeline
CREATE TABLE dbo.DIM_EJECUCION (
    id_ejecucion VARCHAR(50) NOT NULL,             -- UUID generado en extraccion
    id_plataforma VARCHAR(20) NOT NULL,
    version_api VARCHAR(20) NULL,
    fecha_hora_inicio_utc DATETIME2 NOT NULL,
    fecha_hora_fin_utc DATETIME2 NULL,
    duracion_segundos DECIMAL(10,3) NULL,
    estado VARCHAR(20) NOT NULL,                   -- 'EXITO', 'FALLIDA'
    contenidos_extraidos INT NULL DEFAULT 0,
    llamadas_api INT NULL DEFAULT 0,
    paginas_recorridas INT NULL DEFAULT 0,
    cuota_estimada INT NULL DEFAULT 0,
    error_tipo VARCHAR(100) NULL,
    error_codigo INT NULL,
    error_mensaje NVARCHAR(MAX) NULL,
    CONSTRAINT PK_DIM_EJECUCION PRIMARY KEY CLUSTERED (id_ejecucion),
    CONSTRAINT FK_DIM_EJECUCION_PLATAFORMA FOREIGN KEY (id_plataforma) 
        REFERENCES dbo.DIM_PLATAFORMA(id_plataforma)
);

-- ----------------------------------------------------------------------------
-- 3. CREACION DE TABLA DE HECHOS (FACT TABLE)
-- Granularidad: 1 fila por publicacion, por plataforma y por dia de observacion
-- ----------------------------------------------------------------------------
CREATE TABLE dbo.HECHOS_METRICAS_PUBLICACION (
    id_hecho BIGINT IDENTITY(1,1) NOT NULL,
    id_tiempo INT NOT NULL,
    id_plataforma VARCHAR(20) NOT NULL,
    id_cuenta_sk INT NOT NULL,
    id_publicacion_sk INT NOT NULL,
    id_ejecucion VARCHAR(50) NOT NULL,
    
    -- Metricas cuantitativas (admiten NULL segun plataforma de origen)
    cantidad_vistas BIGINT NULL,                   -- YouTube
    cantidad_me_gusta BIGINT NULL,                 -- YouTube / Instagram
    cantidad_comentarios BIGINT NULL,              -- YouTube / Instagram
    cantidad_compartidos BIGINT NULL,              -- Instagram
    cantidad_guardados BIGINT NULL,                -- Instagram
    alcance BIGINT NULL,                           -- Instagram
    impresiones BIGINT NULL,                       -- Instagram
    
    -- Auditoria
    fecha_carga_utc DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_HECHOS_METRICAS_PUBLICACION PRIMARY KEY CLUSTERED (id_hecho),
    CONSTRAINT FK_HECHOS_TIEMPO FOREIGN KEY (id_tiempo) 
        REFERENCES dbo.DIM_TIEMPO(id_tiempo),
    CONSTRAINT FK_HECHOS_PLATAFORMA FOREIGN KEY (id_plataforma) 
        REFERENCES dbo.DIM_PLATAFORMA(id_plataforma),
    CONSTRAINT FK_HECHOS_CUENTA FOREIGN KEY (id_cuenta_sk) 
        REFERENCES dbo.DIM_CUENTA(id_cuenta_sk),
    CONSTRAINT FK_HECHOS_PUBLICACION FOREIGN KEY (id_publicacion_sk) 
        REFERENCES dbo.DIM_PUBLICACION(id_publicacion_sk),
    CONSTRAINT FK_HECHOS_EJECUCION FOREIGN KEY (id_ejecucion) 
        REFERENCES dbo.DIM_EJECUCION(id_ejecucion),
    CONSTRAINT UQ_HECHOS_OBSERVACION UNIQUE (id_publicacion_sk, id_tiempo, id_ejecucion)
);

-- ----------------------------------------------------------------------------
-- 4. INDICES PARA RENDIMIENTO ANALITICO
-- ----------------------------------------------------------------------------
CREATE NONCLUSTERED INDEX IX_HECHOS_TIEMPO 
    ON dbo.HECHOS_METRICAS_PUBLICACION (id_tiempo);

CREATE NONCLUSTERED INDEX IX_HECHOS_PUBLICACION 
    ON dbo.HECHOS_METRICAS_PUBLICACION (id_publicacion_sk);

CREATE NONCLUSTERED INDEX IX_HECHOS_CUENTA_PLATAFORMA 
    ON dbo.HECHOS_METRICAS_PUBLICACION (id_plataforma, id_cuenta_sk);

-- ----------------------------------------------------------------------------
-- 5. SEMILLA INICIAL DE DATOS FIJOS (DIM_PLATAFORMA)
-- ----------------------------------------------------------------------------
INSERT INTO dbo.DIM_PLATAFORMA (id_plataforma, nombre_plataforma, version_api, tipo_fuente)
VALUES 
    ('youtube', 'YouTube', 'v3', 'Google Cloud REST API'),
    ('instagram', 'Instagram', 'v21.0', 'Meta Graph API');
