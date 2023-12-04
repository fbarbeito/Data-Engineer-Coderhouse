delete from {{ params.schema }}.dim_beach
;
insert into {{ params.schema }}.dim_beach
select 
	row_number() over (order by beach) as beachid,
	beach,
	lon,
	lat,
	city_name
from (
select distinct
	beach,
	lon,
	lat,
	city_name 
from 
	{{ params.schema }}.stg_current_weather_uybeach  
) as f
;
insert into {{ params.schema }}.dim_weather
select p.*
from (
select distinct
	weather_id,
	main,
	description 
from 
	{{ params.schema }}.stg_current_weather_uybeach  
union 
select distinct
	weather_id,
	main,
	description 
from 
	{{ params.schema }}.stg_forecast_weather_uybeach  
) p 
	left join {{ params.schema }}.dim_weather dw on dw.weather_id = p.weather_id 
where 
	dw.weather_id is null
;
