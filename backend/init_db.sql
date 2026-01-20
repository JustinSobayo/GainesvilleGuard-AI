-- 1. Enable PostGIS to support GEOMETRY types and spatial queries
CREATE EXTENSION IF NOT EXISTS postgis;

-- 2. Create the raw crime storage table
CREATE TABLE IF NOT EXISTS historical_crimes (
    id                  TEXT PRIMARY KEY,
    incident_type       TEXT,
    description         TEXT,
    
    -- Timestamps
    offense_date        TIMESTAMP,
    report_date         TIMESTAMP,
    
    -- Spatial / Location Data
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    address             TEXT,
    city                TEXT,
    state               TEXT,
    
    -- Feature Engineering Columns (Metadata)
    offense_hour        INTEGER,
    offense_day_of_week TEXT,
    
    -- The Core PostGIS Column (SRID 4326 = GPS Lat/Lon)
    geometry            GEOMETRY(POINT, 4326)
);

-- Optimize spatial queries (e.g., "Find crimes within 1km")
CREATE INDEX IF NOT EXISTS idx_crime_geometry ON historical_crimes USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_crime_offense_date ON historical_crimes (offense_date);


-- 3. Create the Predictive Analysis Cache Table
-- Stores pre-computed risk scores for grid cells to serve APIs instantly
CREATE TABLE IF NOT EXISTS predictive_analysis (
    prediction_id       SERIAL PRIMARY KEY,
    
    -- Spatial Grid Cell (e.g., H3 hex index or a custom grid ID)
    grid_id             TEXT,
    
    -- When is this prediction for? (e.g., "Next Tuesday 3PM")
    prediction_time     TIMESTAMP,
    
    -- The Value: 0.0 to 1.0 Risk Score
    risk_score          DOUBLE PRECISION,
    
    -- Metadata (which model version created this?)
    model_version       TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- Optimize for time-based lookups (e.g., "Show me risk map for NOW")
CREATE INDEX IF NOT EXISTS idx_prediction_time ON predictive_analysis (prediction_time);