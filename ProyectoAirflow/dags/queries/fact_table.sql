insert into  {{ params.schema }}.fact_weather_uybeach
select 
	sfwu.dt::date as date,
	extract('hour' from sfwu.dt) as hour,
	db.beachid, 
	sfwu.sunrise::time as sunrisehour,
	sfwu.sunset::time as sunsethour,
	sfwu.weather_id,
	round(avg(sfwu.temp),2) as temp,
	round(avg(sfwu.feels_like),2) as feels_like,
	round(min(sfwu.temp_min),2) as temp_min,
	round(max(sfwu.temp_max),2) as temp_max,
	round(avg(a.temp),2) as temp_next24h,
	round(avg(a.feels_like),2) as feels_like_next24h,
	round(min(a.temp_min),2) as temp_min_next24h,
	round(max(a.temp_max),2) as temp_max_next24h,
	round(avg(a2.temp),2) as temp_nextfewh,
	round(avg(a2.feels_like),2) as feels_like_nextfewh,
	round(min(a2.temp_min),2) as temp_min_nextfewh,
	round(max(a2.temp_max),2) as temp_max_nextfewh,
	round(avg(a3.temp),2) as temp_sameh_nextday,
	round(avg(a3.feels_like),2) as feels_like_sameh_nextday,
	round(min(a3.temp_min),2) as temp_min_sameh_nextday,
	round(max(a3.temp_max),2) as temp_max_sameh_nextday
from 
	{{ params.schema }}.stg_current_weather_uybeach sfwu
	left join {{ params.schema }}.dim_beach db on db.lon = sfwu.lon and db.lat = sfwu.lat
	left join {{ params.schema }}.stg_forecast_weather_uybeach a on a.city_id = sfwu.city_id and a.dt between sfwu.dt + interval'1 hours' and sfwu.dt + interval'27 hours' 
	left join {{ params.schema }}.stg_forecast_weather_uybeach a2 on a2.city_id = sfwu.city_id and a2.dt between sfwu.dt + interval'1 hours' and sfwu.dt + interval'4 hours' 
	left join {{ params.schema }}.stg_forecast_weather_uybeach a3 on a3.city_id = sfwu.city_id and a3.dt between sfwu.dt + interval'24 hours' and sfwu.dt + interval'27 hours' 
where 
	sfwu.dt::date = (select max(dt) from {{ params.schema }}.stg_current_weather_uybeach)::date 
	and extract('hour' from sfwu.dt) = extract('hour' from  (select max(dt) from {{ params.schema }}.stg_current_weather_uybeach))
group by 1,2,3,4,5,6