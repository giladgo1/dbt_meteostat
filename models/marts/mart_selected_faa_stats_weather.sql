WITH departures_daily AS ( --for departures
    SELECT
        origin AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT dest) AS nunique_to,
        COUNT(sched_dep_time) AS dep_planned,
        SUM(COALESCE(cancelled, 0)) AS dep_cancelled,
        SUM(COALESCE(diverted, 0)) AS dep_diverted,
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS dep_n_flights_calc,
        COUNT(DISTINCT tail_number) AS nunique_tail_number_from,
        COUNT(DISTINCT airline) AS nunique_airline_from
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, flight_date
),
arrivals_daily AS (--for arrivals
    SELECT
        dest AS airport_code,
        flight_date AS date,
        COUNT(DISTINCT origin) AS nunique_from,
        COUNT(sched_arr_time) AS arr_planned,
        SUM(COALESCE(cancelled, 0)) AS arr_cancelled,
        SUM(COALESCE(diverted, 0)) AS arr_diverted,
        SUM(CASE WHEN cancelled = 0 THEN 1 ELSE 0 END) AS arr_n_flights_calc,
        COUNT(DISTINCT tail_number) AS nunique_tail_number_to,
        COUNT(DISTINCT airline) AS nunique_airline_to
    FROM {{ ref('prep_flights') }}
    GROUP BY dest, flight_date
),
airport_daily_stats AS (
    SELECT
        COALESCE(d.airport_code, a.airport_code) AS airport_code,
        COALESCE(d.date, a.date) AS date,
        COALESCE(d.nunique_to, 0) AS nunique_to,
        COALESCE(a.nunique_from, 0) AS nunique_from,
        COALESCE(d.dep_planned, 0) + COALESCE(a.arr_planned, 0) AS total_planned,
        COALESCE(d.dep_cancelled, 0) + COALESCE(a.arr_cancelled, 0) AS total_cancelled,
        COALESCE(d.dep_diverted, 0) + COALESCE(a.arr_diverted, 0) AS total_diverted,
        COALESCE(d.dep_n_flights_calc, 0) + COALESCE(a.arr_n_flights_calc, 0) AS total_n_flights_calc,
        COALESCE(d.nunique_tail_number_from, 0) AS nunique_tail_number_from,
        COALESCE(a.nunique_tail_number_to, 0) AS nunique_tail_number_to,
        COALESCE(d.nunique_airline_from, 0) AS nunique_airline_from,
        COALESCE(a.nunique_airline_to, 0) AS nunique_airline_to,
        (
            COALESCE(d.nunique_tail_number_from, 0)
            + COALESCE(a.nunique_tail_number_to, 0)
        ) / 2.0 AS avg_unique_airplanes,
        (
            COALESCE(d.nunique_airline_from, 0)
            + COALESCE(a.nunique_airline_to, 0)
        ) / 2.0 AS avg_unique_airlines
    FROM departures_daily d
    FULL OUTER JOIN arrivals_daily a
        ON d.airport_code = a.airport_code
       AND d.date = a.date
),
weather_daily AS (
    SELECT
        airport_code,
        date,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction,
        avg_wind_speed,
        avg_peakgust
    FROM {{ ref('prep_weather_daily') }}
)
SELECT
    ads.airport_code,
    ads.date,
    ap.city,
    ap.country,
    ap.name,
    ads.nunique_to,
    ads.nunique_from,
    ads.total_planned,
    ads.total_cancelled,
    ads.total_diverted,
    ads.total_n_flights_calc,
    ads.avg_unique_airplanes,
    ads.avg_unique_airlines,
    wd.min_temp_c,
    wd.max_temp_c,
    wd.precipitation_mm,
    wd.max_snow_mm,
    wd.avg_wind_direction,
    wd.avg_wind_speed,
    wd.avg_peakgust
FROM airport_daily_stats ads
JOIN weather_daily wd
    ON ads.airport_code = wd.airport_code
   AND ads.date = wd.date
LEFT JOIN {{ ref('prep_airports') }} ap
    ON ads.airport_code = ap.faa







--#### 5.3 Flight Route Stats incl. Weather
--In a table `mart_selected_faa_stats_weather.sql` we want to see **for each airport daily**:

-- only the airports we collected the weather data for
-- unique number of departures connections
-- unique number of arrival connections
-- how many flight were planned in total (departures & arrivals)
-- how many flights were canceled in total (departures & arrivals)
-- how many flights were diverted in total (departures & arrivals)
-- how many flights actually occured in total (departures & arrivals)
-- *(optional) how many unique airplanes travelled on average*
-- *(optional) how many unique airlines were in service  on average* 
-- (optional) add city, country and name of the airport
-- daily min temperature
-- daily max temperature
-- daily precipitation 
-- daily snow fall
-- daily average wind direction 
-- daily average wind speed
-- daily wnd peakgust