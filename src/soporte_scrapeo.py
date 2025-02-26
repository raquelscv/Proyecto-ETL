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
    
    dictio_final_hoteles = scrap_info_hoteles(url_selenium, sleep_time=5)
    df_hoteles_competencia = pd.DataFrame(dictio_final_hoteles)
    df_hoteles_competencia.to_pickle(archivo_salida_selenium)

    dictio_final_eventos = scrap_info_eventos(url_api)
    df_eventos = pd.DataFrame(dictio_final_eventos)
    df_eventos.to_pickle(archivo_salida_api)

    return df_hoteles_competencia, df_eventos

