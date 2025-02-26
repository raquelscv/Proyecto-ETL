# 🏨 ETL de Reservas Hoteleras en Madrid 
Este proyecto se centra en la implementación de un proceso ETL (Extract, Transform, Load) para gestionar y estructurar datos de reservas hoteleras en Madrid. Se nos proporcionó un archivo en formato Parquet con información sobre hoteles propios y de la competencia, y nuestro objetivo ha sido migrar estos datos a una base de datos en PostgreSQL (DBeaver), asegurando que la información estuviera limpia, estructurada y lista para su uso.

Para completar el proceso, primero realizamos la extracción de los datos, incorporando información adicional que no estaba presente en el archivo original. Para ello, realizamos un scraping con Selenium para recopilar datos de hoteles de la competencia y utilizamos una API para obtener información sobre eventos en Madrid. 

Una vez recopilada toda la información, llevamos a cabo la fase de transformación, donde nos aseguramos de que los datos fueran consistentes y estuvieran en el formato adecuado. Durante este proceso, tratamos valores nulos, eliminamos duplicados y corregimos inconsistencias, como asignaciones erróneas de identificadores. Con los datos ya limpios y estructurados, procedimos a la carga en la base de datos, garantizando que toda la información estuviera correctamente almacenada y lista para su consulta.

Finalmente, como paso adicional, realizamos algunas consultas SQL para verificar la correcta inserción de los datos y llevar a cabo un pequeño análisis exploratorio. En este análisis, nos centramos en una comparación entre los precios de los hoteles propios y de la competencia, así como en la evolución temporal de las reservas.

## 📂 Estructura del Proyecto

├── data                         # Datos utilizados en el proyecto  
│   ├── reservas_hoteles.parquet      # Datos brutos originales  
│   ├── reservas_hoteles_limpio.pickle  # Datos transformados y limpios  
│   ├── datos_extraidos               # Datos obtenidos mediante scraping y API  
│   │   ├── nombre_estrellas_precio.pickle  # Datos extraídos con Selenium  
│   │   ├── tabla_eventos.pickle            # Datos extraídos de la API  
│  
├── jupyters                     # Notebooks con el desarrollo del proyecto  
│   ├── eda_transf.ipynb          # Exploración, limpieza y transformación de los datos  
│   ├── scrapeo_selenium.ipynb    # Extracción de datos de hoteles con Selenium  
│   ├── scrapeo_api.ipynb         # Extracción de datos de eventos con una API  
│   ├── carga.ipynb               # Carga de los datos en la base de datos  
│   ├── bonus_track_2.ipynb       # Análisis exploratorio final  
│  
├── src                          # Funciones de soporte para la ETL  
│   ├── soporte_limpieza_transf.py  # Funciones para limpieza y transformación  
│   ├── soporte_scrapeo.py         # Funciones para el scraping (Selenium y API)  
│   ├── soporte_carga.py           # Funciones para la carga en la base de datos  
│  
├── Consultas-BD.sql              # Consultas SQL para verificar y analizar los datos  
├── main.py                       # Script principal que ejecuta el proceso ETL completo  
├── planteamiento_proyecto.md      # Documento con el enunciado y estructura de la base de datos  
├── .gitignore                     # Archivo para ignorar archivos innecesarios en el repositorio  
├── README.md                      # Descripción del proyecto  

## 💻 Instalación y Requisitos

Este proyecto ha sido desarrollado en Python 3.13.0 y utiliza las siguientes librerías:
- Manipulación de datos: pandas, numpy, pyarrow
- Visualización de datos: matplotlib, seaborn
- Web Scraping: beautifulsoup4, requests, selenium, webdriver-manager
- Bases de datos: psycopg2, dotenv
- Otros: os, datetime, time

🛠 Configuración adicional: PostgreSQL. Utilizado para la carga de datos en la base de datos.

## 📊 Principales Resultados y Conclusiones del Análisis

- Datos generales: El análisis se basó en reservas realizadas en febrero de 2025 para estancias del 1 al 2 de marzo, con un total de 19 hoteles propios y 10 de la competencia. Se registraron 9,828 reservas en los hoteles propios y 5,172 en los de la competencia.

- Precios por noche: Los hoteles propios tienen un precio medio por noche de 275 euros, más del doble que los 112 euros de los hoteles de la competencia. Aunque los precios de los hoteles propios son más homogéneos, los de la competencia muestran una mayor variabilidad, con una diferencia de hasta 100 euros entre el hotel más caro y el más barato.

- Valoraciones y precios: A pesar de tener precios más altos, los hoteles propios no parecen estar justificados por mejores valoraciones, ya que todos tienen una media de 3 estrellas. Los hoteles de la competencia, en cambio, tienen una media de 4,3 estrellas.

- Momentos de reserva: Las reservas en los hoteles de la competencia se concentran a finales de febrero (21 de febrero), mientras que las de los hoteles propios se distribuyen principalmente entre los primeros días del mes (2-6 de febrero). Esto indica que no es el momento de la reserva lo que justifica los precios elevados de los hoteles propios.

- Recaudación: Los hoteles propios generan una recaudación total mayor debido a sus precios más altos, pero la demanda no parece ser un factor determinante, ya que las reservas son similares entre los hoteles propios y de la competencia. Esto sugiere que los precios elevados de los hoteles propios podrían no ser sostenibles a largo plazo, y podrían beneficiarse de una reevaluación de su estrategia de precios.

## 💡Próximos Pasos

Si hubiera dispuesto de más tiempo, el control de flujo y la gestión de errores podrían haberse mejorado no solo en el archivo main.py, sino en las funciones de las tres fases del proceso ETL (extracción, transformación y carga). En cuanto a la extracción de eventos, habría sido útil ser más detallado en la información obtenida. Por ejemplo, en el horario del evento, incluir también los días de la semana en los que ocurren, aparte de la hora. También, me hubiese gustado haber podido profundizar en la relación entre la ubicación de los hoteles y sus precios. 

Por otro lado, de cara a un futuro, sería interesante explorar cómo automatizar la extracción de datos y la carga a la base de datos para facilitar la actualización periódica de los datos sin intervención manual.

## 🤝 Contribuciones
Agradezco cualquier contribución que pueda mejorar el proyecto. Si tienes alguna idea que aportar no dudes en contactar conmigo!
- LinkedIn: www.linkedin.com/in/raquelsanchezcv 
- Correo electrónico: raquelscv@gmail.com

## 👤 Autor 
**Raquel Sánchez** - https://github.com/raquelscv 
