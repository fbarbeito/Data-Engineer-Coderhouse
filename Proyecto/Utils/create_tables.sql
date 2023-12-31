CREATE TABLE IF NOT EXISTS {schema}.stg_current_weather_uybeach
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

CREATE TABLE IF NOT EXISTS {schema}.stg_forecast_weather_uybeach
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

CREATE TABLE IF NOT EXISTS {schema}.dim_beach
(
    beachid INT,
	beach VARCHAR(50),
	lon FLOAT,
	lat FLOAT,
	city_name VARCHAR(50)
) DISTKEY(beachid) 

;

CREATE TABLE IF NOT EXISTS {schema}.dim_weather
(
    weather_id INT,
	main VARCHAR(50),
	description VARCHAR(50)
) DISTKEY(weather_id) 
;