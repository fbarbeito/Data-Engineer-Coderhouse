insert into {schema}.dim_weather
select p.*
from (
select distinct
	weather_id,
	main,
	description 
from 
	{schema}.stg_current_weather_uybeach  
union 
select distinct
	weather_id,
	main,
	description 
from 
	{schema}.stg_forecast_weather_uybeach  
) p 
	left join {schema}.dim_weather dw on dw.weather_id = p.weather_id 
where 
	dw.weather_id is null
;