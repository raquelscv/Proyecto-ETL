import pandas as pd 
import requests 
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from datetime import datetime 
import time


def scrap_info_hoteles(url, sleep_time=5):
    """
    Extrae información sobre hoteles desde una página web utilizando Selenium.

    Esta función navega a través de la página web de los hoteles y extrae información sobre el nombre del hotel, 
    el precio por noche, las estrellas y la fecha de reserva. Utiliza Selenium para interactuar con la página y 
    obtiene los datos necesarios para crear un diccionario de resultados.

    Args:
        url (str): La URL de la página web de los hoteles a scrapear.
        sleep_time (int, optional): El tiempo en segundos para esperar antes de comenzar a extraer la información. 
                                     Por defecto es 5 segundos.

    Returns:
        dict: Un diccionario con las claves "nombre_hotel", "estrellas", "precio_noche", y "fecha_reserva" 
              y sus correspondientes listas de datos extraídos de la página web.

    Raises:
        Exception: Si ocurre un error al intentar extraer información de la página web.
    """
    dictio_scrap = {
        "nombre_hotel": [],  
        "estrellas": [],  
        "precio_noche": [], 
        "fecha_reserva": []  
    }
    
    service = Service(ChromeDriverManager().install())  
    options = Options() 
    options.add_argument("--start-maximized")  
    driver = webdriver.Chrome(service=service, options=options)  

    driver.get(url) 
    time.sleep(sleep_time)  
    
    try:
        hoteles = driver.find_elements(By.CLASS_NAME, "hotelblock")

        for hotel in hoteles:
            nombre = hotel.find_element(By.CLASS_NAME, "title").text.split("\n")[0]    
            dictio_scrap["nombre_hotel"].append(nombre)  

            precio_texto = hotel.find_element(By.CLASS_NAME, "rate-details__price-wrapper").text.split("\n")[1].replace("€", "")
            precio = float(precio_texto)
            dictio_scrap["precio_noche"].append(precio)  

            estrellas_texto = hotel.find_element(By.CLASS_NAME, "ratings__score").text.split("/")[0]
            estrellas = float(estrellas_texto)
            dictio_scrap["estrellas"].append(estrellas)  

            dictio_scrap["fecha_reserva"].append(pd.Timestamp(datetime.now().date()))

    except Exception as e:
        print("Error al extraer la información:", e)

    driver.quit()  
    
    return dictio_scrap


def scrap_info_eventos(url):
    """
    Extrae información sobre eventos desde una API pública.

    Esta función obtiene información sobre eventos en la ciudad de Madrid desde una API, incluyendo detalles como el 
    nombre del evento, su URL, la dirección, el horario y las fechas de inicio y fin. Filtra los eventos para que 
    solo incluya aquellos que ocurren en el rango de fechas especificado.

    Args:
        url (str): La URL de la API de eventos desde donde se extraerán los datos.

    Returns:
        dict: Un diccionario con las claves "nombre_evento", "url_evento", "codigo_postal", "direccion", 
              "horario", "organizacion", "inicio_evento", "fin_evento", y "ciudad", y sus correspondientes 
              listas con los datos extraídos de la API.
    """
    response = requests.get(url)
    data = response.json() 

    info_eventos = {
        "nombre_evento": [],
        "url_evento": [],
        "codigo_postal": [],
        "direccion": [],
        "horario": [],
        "organizacion": [],
        "inicio_evento": [],
        "fin_evento": [],
        "ciudad": []
    }

    for evento in data["@graph"]:
        start_date = pd.to_datetime(evento.get('dtstart'))
        end_date = pd.to_datetime(evento.get('dtend'))

        if start_date <= pd.to_datetime("2025-03-02 23:59:00") and end_date >= pd.to_datetime("2025-03-01 00:00:00"):
            info_eventos["nombre_evento"].append(evento.get('title', None))
            info_eventos["url_evento"].append(evento.get('link', None))
            address = evento.get("address", {})
            area = address.get("area", {}) 
            info_eventos["codigo_postal"].append(area.get("postal-code", None))
            info_eventos["direccion"].append(area.get("street-address", None))
            info_eventos["horario"].append(evento.get("time", "") or None)
            organizacion = evento.get('organization', {})
            info_eventos["organizacion"].append(organizacion.get('organization-name', None))
            info_eventos["inicio_evento"].append(evento.get("dtstart", None).split(" ")[0] if evento.get("dtstart") else None)
            info_eventos["fin_evento"].append(evento.get("dtend", None).split(" ")[0] if evento.get("dtend") else None)
            info_eventos["ciudad"].append("Madrid")
            
    return info_eventos

def extraer_datos(url_selenium, url_api, archivo_salida_selenium, archivo_salida_api):
    """
    Extrae datos sobre hoteles y eventos desde fuentes web y guarda los resultados en archivos.

    Esta función utiliza dos funciones de scraping diferentes para obtener información sobre hoteles desde una página 
    web con Selenium y sobre eventos desde una API pública. Los datos extraídos se guardan en archivos de salida 
    (en formato Pickle) para ser utilizados posteriormente.

    Pasos realizados por la función:
    1. Verifica que las URLs proporcionadas sean válidas y de tipo cadena.
    2. Realiza el scraping de información sobre los hoteles utilizando la función `scrap_info_hoteles`.
    3. Guarda los datos de los hoteles en el archivo especificado (`archivo_salida_selenium`).
    4. Realiza el scraping de información sobre los eventos utilizando la función `scrap_info_eventos`.
    5. Guarda los datos de los eventos en el archivo especificado (`archivo_salida_api`).
    
    Args:
        url_selenium (str): URL de la página web de los hoteles para realizar el scraping con Selenium.
        url_api (str): URL de la API de eventos para realizar el scraping.
        archivo_salida_selenium (str): Ruta del archivo donde se guardarán los datos de los hoteles extraídos.
        archivo_salida_api (str): Ruta del archivo donde se guardarán los datos de los eventos extraídos.

    Returns:
        tuple: Una tupla con dos DataFrames:
            - El primer DataFrame contiene los datos de los hoteles.
            - El segundo DataFrame contiene los datos de los eventos.
    
    Raises:
        ValueError: Si las URLs proporcionadas no son válidas o no se puede acceder a ellas.
        FileNotFoundError: Si no se pueden guardar los archivos de salida.
    """
    
    # verifica que las URLs de entrada sean validas
    if not isinstance(url_selenium, str) or not isinstance(url_api, str):
        raise ValueError("Las URLs proporcionadas deben ser de tipo cadena de texto.")
    
    # scraping de datos de los hoteles
    try:
        dictio_final_hoteles = scrap_info_hoteles(url_selenium, sleep_time=5)
    except Exception as e:
        raise ValueError(f"Error al extraer datos de los hoteles: {e}")

    df_hoteles_competencia = pd.DataFrame(dictio_final_hoteles)
    
    # guardar los datos de los hoteles en un archivo Pickle
    try:
        df_hoteles_competencia.to_pickle(archivo_salida_selenium)
    except Exception as e:
        raise FileNotFoundError(f"No se pudo guardar el archivo de salida de los hoteles: {e}")

    # scraping de datos de los eventos
    try:
        dictio_final_eventos = scrap_info_eventos(url_api)
    except Exception as e:
        raise ValueError(f"Error al extraer datos de los eventos: {e}")

    df_eventos = pd.DataFrame(dictio_final_eventos)
    
    # guardar los datos de los eventos en un archivo Pickle
    try:
        df_eventos.to_pickle(archivo_salida_api)
    except Exception as e:
        raise FileNotFoundError(f"No se pudo guardar el archivo de salida de los eventos: {e}")

    return df_hoteles_competencia, df_eventos





