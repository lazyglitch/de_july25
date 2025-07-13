-- Создание бд
SELECT 'CREATE DATABASE openmeteo_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmeteo_db');

-- Подключение к бд
\c openmeteo_db;

-- Создание таблицы
CREATE TABLE IF NOT EXISTS daily_forecast (

    avg_temperature_2m_24h REAL,
    avg_relative_humidity_2m_24h INTEGER,
    avg_dew_point_2m_24h REAL,
    avg_apparent_temperature_24h REAL,
    avg_temperature_80m_24h REAL,
    avg_temperature_120m_24h REAL,
    avg_wind_speed_10m_24h REAL,
    avg_wind_speed_80m_24h REAL,
    avg_visibility_24h REAL,
    total_rain_24h REAL,
    total_showers_24h REAL,
    total_snowfall_24h REAL,
    avg_temperature_2m_daylight REAL,
    avg_relative_humidity_2m_daylight INTEGER,
    avg_dew_point_2m_daylight REAL,
    avg_apparent_temperature_daylight REAL,
    avg_temperature_80m_daylight REAL,
    avg_temperature_120m_daylight REAL,
    avg_wind_speed_10m_daylight REAL,
    avg_wind_speed_80m_daylight REAL,
    avg_visibility_daylight REAL,
    total_rain_daylight REAL,
    total_showers_daylight REAL,
    total_snowfall_daylight REAL,

    wind_speed_10m_m_per_s REAL[],
    wind_speed_80m_m_per_s REAL[],
    temperature_2m_celsius REAL[],
    apparent_temperature_celsius REAL[],
    temperature_80m_celsius REAL[],
    temperature_120m_celsius REAL[],
    soil_temperature_0cm_celsius REAL[],
    soil_temperature_6cm_celsius REAL[],
    rain_mm REAL[],
    showers_mm REAL[],
    snowfall_mm REAL[],

    daylight_hours INTEGER,
    sunset_iso TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    sunrise_iso TIMESTAMP WITHOUT TIME ZONE NOT NULL,

    -- Чтобы предотвратить вставку дубликатов используем уникальный ключ sunrise_iso + sunset_iso
    -- Предполагаем, что вероятность совпадения этих значений у разных дат крайне мала
    UNIQUE (sunrise_iso, sunset_iso)
);