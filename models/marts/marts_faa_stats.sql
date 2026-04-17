SELECT * FROM prep_flights;
--airport as departure
SELECT origin, 
count(DISTINCT dest)AS nuniqe_to,
count(sched_dep_time)AS dep_planned
FROM prep_flights
GROUP BY origin;

--airport as destination
SELECT origin, 
count(DISTINCT origin)AS nuniqe_from,
count(sched_arr_time)AS arr_planned
FROM prep_flights
GROUP BY dest;

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