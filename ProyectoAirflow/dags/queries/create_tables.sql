CREATE TABLE IF NOT EXISTS {{ params.schema }}.stg_current_weather_uybeach
(
    dt TIMESTAMP,
    beach VARCHAR(50),
    country CHAR(3),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    lon FLOAT,
    lat FLOAT,
    city_id INT,
    city_name VARCHAR(50),
    weather_id INT,
    main VARCHAR(50),
    description VARCHAR(50),
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    humidity INT,
    speed FLOAT,
    deg INT,
    gust FLOAT,
    visibility INT,
    clouds INT
) DISTKEY(city_id) SORTKEY(dt)
;

CREATE TABLE IF NOT EXISTS {{ params.schema }}.stg_forecast_weather_uybeach
(
    dt TIMESTAMP,
    beach VARCHAR(50),
    country CHAR(3),
    sunrise TIMESTAMP,
    sunset TIMESTAMP,
    lon FLOAT,
    lat FLOAT,
    city_id INT,
    city_name VARCHAR(50),
    weather_id INT,
    main VARCHAR(50),
    description VARCHAR(50),
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure INT,
    humidity INT,
    speed FLOAT,
    deg INT,
    gust FLOAT,
    visibility INT,
    clouds INT
) DISTKEY(city_id) SORTKEY(dt)
;

CREATE TABLE IF NOT EXISTS {{ params.schema }}.dim_beach
(
    beachid INT,
	beach VARCHAR(50),
	lon FLOAT,
	lat FLOAT,
	city_name VARCHAR(50)
) DISTKEY(beachid) 

;

CREATE TABLE IF NOT EXISTS {{ params.schema }}.dim_weather
(
    weather_id INT,
	main VARCHAR(50),
	description VARCHAR(50)
) DISTKEY(weather_id) 
;

CREATE TABLE IF NOT EXISTS  {{ params.schema }}.fact_weather_uybeach
(
    date DATE,
    hour INT,
    beachid INT,
    sunrisehour TIME,
    sunsethour TIME,
    weather_id INT,
    temp FLOAT,
    feels_like FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    temp_next24h FLOAT,
    feels_like_next24h FLOAT,
    temp_min_next24h FLOAT,
    temp_max_next24h FLOAT,
    temp_nextfewh FLOAT,
    feels_like_nextfewh FLOAT,
    temp_min_nextfewh FLOAT,
    temp_max_nextfewh FLOAT,
    temp_sameh_nextday FLOAT,
    feels_like_sameh_nextday FLOAT,
    temp_min_sameh_nextday FLOAT,
    temp_max_sameh_nextday FLOAT
) DISTKEY(weather_id)  SORTKEY(beachid)