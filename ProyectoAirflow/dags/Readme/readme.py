dag_principal = """
### Proyecto de extracción de datos de openweather  

### Objetivo 

La idea es extraer datos de 10 balnearios uruguayos y obtener la temperatura actual al momento de la extracción y 
la predicción a 3 días.
El producto final será: 
- Una fact table que muestre por hora de extracción y playa (beachid), el estado del tiempo (weather_id), la temperatura actual, la sensación térmica, el mínimo y el máximo, y predicciones para las próximas 24 horas, para las próximas horas y para la mismo tramo horario el día siguiente.
- Dos Tablas de dimensiones: para el estado del tiempo (dim_weather) y para características de la playa (dim_beach)
- De esta forma, pueden integrarse en una herramienta de BI como un Star Schema de forma óptima

### Metodología

Se utilizan distintas herramientas. 
- La principal es Airflow que se ejecuta mediante Docker.
- Sentencias SQL, para crear tablas y poblar las fact y dim tables.
- Python para extraer datos de la API (https://openweathermap.org/api/) e impactarlas en las tablas de stagging.

### Requerimientos 

- Con Airflow: Ejecutar la imagen de Docker y crear la conexión de Postgres para que se ejecuten las queries
- main.py: La ejecución funciona brindando el archivo config.ini que tiene las credenciales.
"""