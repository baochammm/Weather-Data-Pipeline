CREATE DATABASE weather_db;

USE weather_db;

-- Table contains raw weather data from API
CREATE TABLE stg_weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city_name VARCHAR(50),
    raw_json JSON,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table contains cleaned and transformed weather data
CREATE TABLE fact_weather (
    id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50),
    temperature FLOAT,
    humidity INT,
    condition_desc VARCHAR(100),
    recorded_at DATETIME
);