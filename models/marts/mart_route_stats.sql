WITH route_stats AS (
    SELECT
        origin AS origin_airport_code,
        dest AS destination_airport_code,
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_airplanes,
        COUNT(DISTINCT airline) AS unique_airlines,
        AVG(actual_elapsed_time) AS avg_actual_elapsed_time,
        AVG(arr_delay) AS avg_arr_delay,
        MAX(arr_delay) AS max_arr_delay,
        MIN(arr_delay) AS min_arr_delay,
        SUM(COALESCE(cancelled, 0)) AS total_cancelled,
        SUM(COALESCE(diverted, 0)) AS total_diverted
    FROM {{ ref('prep_flights') }}
    GROUP BY origin, dest
)
SELECT
    rs.origin_airport_code,
    ap_origin.city AS origin_city,
    ap_origin.country AS origin_country,
    ap_origin.name AS origin_airport_name,
    rs.destination_airport_code,
    ap_dest.city AS destination_city,
    ap_dest.country AS destination_country,
    ap_dest.name AS destination_airport_name,
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_actual_elapsed_time,
    rs.avg_arr_delay,
    rs.max_arr_delay,
    rs.min_arr_delay,
    rs.total_cancelled,
    rs.total_diverted
FROM route_stats rs
LEFT JOIN {{ ref('prep_airports') }} ap_origin
    ON rs.origin_airport_code = ap_origin.faa
LEFT JOIN {{ ref('prep_airports') }} ap_dest
    ON rs.destination_airport_code = ap_dest.faa


--#### 5.2 Flight Route Stats

--In a table `mart_route_stats.sql` we want to see **for each route over all time**:

--     origin airport code
--     destination airport code 
--     total flights on this route
--     unique airplanes
--     unique airlines
--     on average what is the actual elapsed time
--     on average what is the delay on arrival
--    what was the max delay?
--    what was the min delay?
--    total number of cancelled 
--    total number of diverted
-- add city, country and name for both, origin and destination, airports