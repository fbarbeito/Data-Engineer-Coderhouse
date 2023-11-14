delete from barbeito26_coderhouse.dim_beach
;
insert into barbeito26_coderhouse.dim_beach
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
	barbeito26_coderhouse.stg_current_weather_uybeach  
) as f
;