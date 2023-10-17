--- Creo una función que me devuelva los cuatrienios para un rango seleccionado
--- Si no es un rango adecuado devuelve error

create or replace function cuatrienio(añoinicio int,añofinal int)
returns table (año int,cuatrienio varchar)
language plpgsql
as 
$$
begin
	if (añofinal-añoinicio+1)/4::numeric - (añofinal-añoinicio+1)/4 <> 0 then 
	raise exception 'no hay cuatro años en el rango de años seleccionados.';
	end if;
	return 	query
select 
	anio as año,
	((min(anio) over (partition by cuatrienio_index))::varchar || '-' || (max(anio) over (partition by cuatrienio_index))::varchar)::varchar as cuatrienio
from(
select 
	generate_series(añoinicio,añofinal,1) as anio,
	ceil((row_number() over (order by generate_series(añoinicio,añofinal,1) asc))/4::numeric) as cuatrienio_index
) as f ;
end;
$$;

--- Diseño de etl que inserta los registros en la tabla denormalizada
--- Observar que la inserción puede ser incremental
--- Insertar los registros originales y luego los nuevos
--- El SP funciona ok

create or replace procedure pETL_Desastres(añoinicio int,añofinal int)
language plpgsql
as 
$$
begin
	if (añofinal-añoinicio+1)/4::numeric - (añofinal-añoinicio+1)/4 <> 0 then 
	raise exception 'no hay cuatro años en el rango de años seleccionados.';
	end if;
delete 
from 
	desastres_final
where 
	cuatrenio in (
	select distinct  cuatrienio
	from  
		desastres_final d 
		left join public.cuatrienio(añoinicio,añofinal) as a on a.cuatrienio = d.cuatrenio
	where 
		año>= añoinicio 
		and año<= añofinal
	)
;
insert into desastres_final
select
	* 
from  
	dblink('dbname=desastres  user=postgres password= hostaddr=127.0.0.1',
	'
	select
		c2.cuatrienio as cuatrenio,
		avg(temperatura) as Temp_AVG,
		avg(oxigeno) as Oxi_AVG,
		sum(d.tsunamis) as T_Tsunamis,
		sum(olas_calor) as T_OlasCalor,
		sum(terremotos) as T_Terremotos,
		sum(erupciones) as	T_Erupciones,
		sum(incendios) as  T_Incendios,
		avg(m.r_menor15 + m.r_15_a_30) as M_Jovenes_AVG,
		avg(m.r_30_a_45 + m.r_45_a_60) as M_Adutos_AVG,
		avg(m.r_m_a_60) as M_Ancianos_AVG
	from 
		desastres d
		full join clima c on c.año = d.año 
		full join muertes m on m.año = d.año 
		full join cuatrienio(' || añoinicio::varchar || ',' || añofinal::varchar || ') as c2 on c2.año=d.año
	where
		coalesce(d.año,c.año,m.año) >= ' || añoinicio::varchar || ' 
		and coalesce(d.año,c.año,m.año) <= ' || añofinal::varchar  || '
	group by 1
	') as uu(Cuatrenio varchar(20),
		Temp_AVG FLOAT, 
		Oxi_AVG FLOAT ,
		T_Tsunamis INT  , T_OlasCalor INT,
		T_Terremotos INT, T_Erupciones INT  , 
		T_Incendios INT ,M_Jovenes_AVG FLOAT  ,
		M_Adutos_AVG FLOAT ,M_Ancianos_AVG FLOAT);
end;
$$;

call pETL_Desastres(2023,2030)
;
select *
from 
	desastres_final df 
;

call pETL_Desastres(2031,2034)
;

select *
from 
	desastres_final df 


