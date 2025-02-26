import os 
from dotenv import load_dotenv
from src.soporte_carga import crear_tablas
from src.soporte_scrapeo import extraer_datos
from src.soporte_limpieza_transf import limpieza_transformacion

load_dotenv()
ARCHIVO_RAW = os.getenv("ARCHIVO_RAW")
ARCHIVO_SALIDA = os.getenv("ARCHIVO_SALIDA")
URL_SELENIUM = os.getenv("URL_SELENIUM")
URL_API = os.getenv("URL_API")
ARCHIVO_EXTRACCION_HOTELES = os.getenv("ARCHIVO_EXTRACCION_HOTELES")
ARCHIVO_EXTRACCION_EVENTOS = os.getenv("ARCHIVO_EXTRACCION_EVENTOS")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def extraccion_transf_carga(archivo_entrada, archivo_salida, url_selenium, url_api, archivo_salida_selenium, archivo_salida_api, nombre_db, usuario, contraseña, servidor, puerto):
    df_hoteles_competencia, df_eventos = extraer_datos(url_selenium, url_api, archivo_salida_selenium, archivo_salida_api)
    df = limpieza_transformacion(archivo_entrada, df_hoteles_competencia, archivo_salida)
    crear_tablas(nombre_db, usuario, contraseña, servidor, puerto, df, df_eventos, df_hoteles_competencia)

extraccion_transf_carga(ARCHIVO_RAW, ARCHIVO_SALIDA, URL_SELENIUM, URL_API, ARCHIVO_EXTRACCION_HOTELES, ARCHIVO_EXTRACCION_EVENTOS, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)

