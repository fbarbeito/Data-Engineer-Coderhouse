create database desastres ; --- cre la base donde va las transacciones

CREATE TABLE clima
(año INT NOT NULL PRIMARY KEY,
Temperatura FLOAT NOT NULL,
Oxigeno FLOAT NOT NULL);

-- Insertar valores manualmente
INSERT INTO clima VALUES (2023, 22.5,230);
INSERT INTO clima VALUES (2024, 22.7,228.6);
INSERT INTO clima VALUES (2025, 22.9,227.5);
INSERT INTO clima VALUES (2026, 23.1,226.7);
INSERT INTO clima VALUES (2027, 23.2,226.4);
INSERT INTO clima VALUES (2028, 23.4,226.2);
INSERT INTO clima VALUES (2029, 23.6,226.1);
INSERT INTO clima VALUES (2030, 23.8,225.1);

-- 3. crear tabla desastres proyectados globales
CREATE TABLE desastres
(año INT NOT NULL PRIMARY KEY,
Tsunamis INT NOT NULL,
Olas_Calor INT NOT NULL,
Terremotos INT NOT NULL,
Erupciones INT NOT NULL,
Incendios INT NOT NULL);

-- Insertar valores manualmente
INSERT INTO desastres VALUES (2023, 2,15, 6,7,50);
INSERT INTO desastres VALUES (2024, 1,12, 8,9,46);
INSERT INTO desastres VALUES (2025, 3,16, 5,6,47);
INSERT INTO desastres VALUES (2026, 4,12, 10,13,52);
INSERT INTO desastres VALUES (2027, 5,12, 6,5,41);
INSERT INTO desastres VALUES (2028, 4,18, 3,2,39);
INSERT INTO desastres VALUES (2029, 2,19, 5,6,49);
INSERT INTO desastres VALUES (2030, 4,20, 6,7,50);

-- 4. crear tabla muertes proyectadas por rangos de edad
CREATE TABLE muertes
(año INT NOT NULL PRIMARY KEY,
R_Menor15 INT NOT NULL,
R_15_a_30 INT NOT NULL,
R_30_a_45 INT NOT NULL,
R_45_a_60 INT NOT NULL,
R_M_a_60 INT NOT NULL);
GO
-- Insertar valores manualmente
INSERT INTO muertes VALUES (2023, 1000,1300, 1200,1150,1500);
INSERT INTO muertes VALUES (2024, 1200,1250, 1260,1678,1940);
INSERT INTO muertes VALUES (2025, 987,1130, 1160,1245,1200);
INSERT INTO muertes VALUES (2026, 1560,1578, 1856,1988,1245);
INSERT INTO muertes VALUES (2027, 1002,943, 1345,1232,986);
INSERT INTO muertes VALUES (2028, 957,987, 1856,1567,1756);
INSERT INTO muertes VALUES (2029, 1285,1376, 1465,1432,1236);
INSERT INTO muertes VALUES (2030, 1145,1456, 1345,1654,1877);

-- 5. Crear base de datos para alojar resumenes de estadisticas
CREATE DATABASE DESASTRES_BDE;

CREATE TABLE DESASTRES_FINAL
(Cuatrenio varchar(20) NOT NULL PRIMARY KEY,
Temp_AVG FLOAT NOT NULL, Oxi_AVG FLOAT NOT NULL,
T_Tsunamis INT NOT NULL, T_OlasCalor INT NOT NULL,
T_Terremotos INT NOT NULL, T_Erupciones INT NOT NULL, 
T_Incendios INT NOT NULL,M_Jovenes_AVG FLOAT NOT NULL,
M_Adutos_AVG FLOAT NOT NULL,M_Ancianos_AVG FLOAT NOT NULL);

--- Función que devuelve el cuatrienio

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

---- Nuevas inserciones 

INSERT INTO muertes VALUES (2031, 1123, 1345, 1234, 998, 1023);
INSERT INTO muertes VALUES (2032, 976, 1005, 1798, 1523, 1724);
INSERT INTO muertes VALUES (2033, 1222, 1311, 1433, 1400, 1267);
INSERT INTO muertes VALUES (2034, 1190, 1434, 1312, 1611, 1845);

INSERT INTO desastres VALUES (2031, 6, 14, 7, 8, 45);
INSERT INTO desastres VALUES (2032, 5, 15, 8, 7, 42);
INSERT INTO desastres VALUES (2033, 3, 16, 6, 9, 48);
INSERT INTO desastres VALUES (2034, 4, 17, 9, 10, 55);

INSERT INTO clima VALUES (2031, 23.1, 226.3);
INSERT INTO clima VALUES (2032, 23.3, 226.0);
INSERT INTO clima VALUES (2033, 23.5, 225.9);
INSERT INTO clima VALUES (2034, 23.7, 225.8);
