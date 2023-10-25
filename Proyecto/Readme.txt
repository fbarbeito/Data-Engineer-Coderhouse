** Proyecto de extracción de datos de openweather **

** Objetivo ** 
La idea es extraer datos de 10 balnearios uruguayos y obtener la temperatura actual al momento de la extracción y 
la predicción a 3 días cada tres horas (la idea es posteriormente hacer promedios, máximos y mínimos por fecha al momento de la 
extracción; de esa forma un usuario puede ver cómo se encuentra el tiempo actual y diferentes predicciones consultando una aplicación
que consuma la data).

** Método ** 
Al momento scripts de python y de SQL.

** scripts **
main.py: ejecuta etl_staging.py y create_tables.py 
create_tables.py: crea las tablas de staging (en este entregable) que se pueblan con los datos extraídos de la API. El script encargado 
es etl_staging. Consume es script sql create_tables.sql.
etl_staging.py: extrae datos de la API https://openweathermap.org/api, hace las transformaciones necesarias y las impacta
en las tablas de staging en Amazon Redfshit.
