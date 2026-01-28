-- Step 1: Move cleaned data from Staging to Fact table
INSERT INTO
    fact_weather (
        city,
        temperature,
        humidity,
        condition_desc,
        recorded_at
    )
SELECT city_name, JSON_EXTRACT(raw_json, '$.main.temp') - 273.15, JSON_EXTRACT(raw_json, '$.main.humidity'), JSON_UNQUOTE(
        JSON_EXTRACT(
            raw_json, '$.weather[0].description'
        )
    ), NOW()
FROM stg_weather;

-- Step 2: Clear staging table after transformation
TRUNCATE TABLE stg_weather;