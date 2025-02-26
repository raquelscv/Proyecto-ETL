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
    """
    Realiza el proceso completo de extracción, transformación y carga de datos, integrando todas las funciones 
    necesarias para extraer los datos, limpiarlos, transformarlos y cargarlos en la base de datos.

    Este proceso sigue los siguientes pasos:
        1. Extrae los datos de los hoteles de la competencia y eventos mediante Selenium y la API.
        2. Realiza la limpieza y transformación de los datos extraídos.
        3. Crea las tablas necesarias en la base de datos y carga los datos transformados.

    Args:
        archivo_entrada (str): Ruta al archivo de entrada (datos sin procesar) que contiene la información de las reservas.
        archivo_salida (str): Ruta al archivo donde se guardará el DataFrame transformado (en formato Pickle).
        url_selenium (str): URL para extraer los datos de los hoteles de la competencia usando Selenium.
        url_api (str): URL para obtener los datos de eventos a través de la API.
        archivo_salida_selenium (str): Ruta al archivo donde se guardarán los datos extraídos con Selenium.
        archivo_salida_api (str): Ruta al archivo donde se guardarán los datos extraídos de la API.
        nombre_db (str): Nombre de la base de datos donde se cargarán los datos.
        usuario (str): Nombre de usuario para la conexión a la base de datos.
        contraseña (str): Contraseña para la conexión a la base de datos.
        servidor (str): Dirección del servidor donde se encuentra la base de datos.
        puerto (int): Puerto en el que la base de datos está escuchando.

    Returns:
        None: Esta función no retorna ningún valor, solo realiza el proceso completo de extracción, transformación y carga.

    Raises:
        Exception: Si ocurre un error en alguna de las etapas (extracción, transformación, carga), se lanzará una excepción 
                   con un mensaje detallado.
    """
    try:
        # extraer los datos de hoteles y eventos
        try:
            df_hoteles_competencia, df_eventos = extraer_datos(url_selenium, url_api, archivo_salida_selenium, archivo_salida_api)
        except Exception as e:
            raise Exception(f"Error en la extracción de datos: {e}")

        # realizar la limpieza y transformacion de los datos
        try:
            df = limpieza_transformacion(archivo_entrada, df_hoteles_competencia, archivo_salida)
        except Exception as e:
            raise Exception(f"Error en la limpieza y transformación de datos: {e}")

        # crear tablas y cargar datos en la base de datos
        try:
            crear_tablas(nombre_db, usuario, contraseña, servidor, puerto, df, df_eventos, df_hoteles_competencia)
        except Exception as e:
            raise Exception(f"Error en la creación de tablas y carga de datos en la base de datos: {e}")

    except Exception as e:
        # en caso de error en alguna de las etapas, se lanza una excepcion con un mensaje detallado
        raise Exception(f"Error en el proceso completo de extracción, transformación y carga: {e}")

# llamada a la funcion final que realiza el proceso completo de extraccion, transformacion y carga
extraccion_transf_carga(ARCHIVO_RAW, ARCHIVO_SALIDA, URL_SELENIUM, URL_API, ARCHIVO_EXTRACCION_HOTELES, ARCHIVO_EXTRACCION_EVENTOS, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT)
