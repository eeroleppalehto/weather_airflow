-- SQL script to create historical_weather database and tables
-- To execute this script in postgres, run:
-- psql -f create_weather_tables.sql


-- Create database "historical_weather"
CREATE DATABASE historical_weather;


-- Connect to the database
\c historical_weather


-- =============================
-- Create daily weather table
-- =============================
CREATE TABLE daily_weather (
    id SERIAL PRIMARY KEY,
    formatted_date TIMESTAMPTZ,             -- Date and time with timezone
    precip_type TEXT,
    temperature_c DOUBLE PRECISION,
    apparent_temperature_c DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed_kmh DOUBLE PRECISION,
    visibility_km DOUBLE PRECISION,
    pressure_millibars DOUBLE PRECISION,
    wind_strength TEXT,
    avg_temperature_c DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_wind_speed_kmh DOUBLE PRECISION
);


-- =============================
-- Create monthly weather table
-- =============================
CREATE TABLE monthly_weather (
    id SERIAL PRIMARY KEY,
    month DATE,                             -- Month identifier (e.g., 2025-11-01)
    avg_temperature_c DOUBLE PRECISION,
    avg_apparent_temperature_c DOUBLE PRECISION,
    avg_humidity DOUBLE PRECISION,
    avg_visibility_km DOUBLE PRECISION,
    avg_pressure_millibars DOUBLE PRECISION,
    mode_precip_type TEXT
);