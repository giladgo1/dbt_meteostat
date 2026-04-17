WITH weather_weekly AS (
    SELECT
        airport_code,
        station_id,
        DATE_TRUNC('week', date)::date AS week_start_date,
        AVG(avg_temp_c) AS avg_temp_c,
        MIN(min_temp_c) AS min_temp_c,
        MAX(max_temp_c) AS max_temp_c,
        SUM(precipitation_mm) AS precipitation_mm,
        SUM(max_snow_mm) AS snowfall_mm,
        ROUND(AVG(avg_wind_direction))::integer AS avg_wind_direction,
        AVG(avg_wind_speed) AS avg_wind_speed,
        MAX(avg_peakgust) AS max_peakgust,
        AVG(avg_pressure_hpa) AS avg_pressure_hpa,
        SUM(sun_minutes) AS sun_minutes
    FROM {{ ref('prep_weather_daily') }}
    GROUP BY
        airport_code,
        station_id,
        DATE_TRUNC('week', date)::date
)
SELECT *
FROM weather_weekly








---#### 5.4 Weekly weather

--In a table `mart_weather_weekly.sql` we want to see **all** 
--weather stats from the `prep_weather_daily` model aggregated weekly. 

-- consider whether the metric should be Average, Maximum, Minimum, 
--Sum or [Mode](https://wiki.postgresql.org/wiki/Aggregate_Mode)