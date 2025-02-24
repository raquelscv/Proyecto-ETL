import pandas as pd 
import os 
from dotenv import load_dotenv
from src.soporte_excrapeo import scrap_info_hoteles, scrap_info_eventos

load_dotenv()
URL_SELENIUM = os.getenv("URL_SELENIUM")
URL_API = os.getenv("URL_API")
ARCHIVO_EXTRACCION_HOTELES = os.getenv("ARCHIVO_EXTRACCION_HOTELES")
ARCHIVO_EXTRACCION_EVENTOS = os.getenv("ARCHIVO_EXTRACCION_EVENTOS")

def scrapeo_total(url_selenium, url_api, archivo_salida_selenium, archivo_salida_api):
    dictio_final_hoteles = scrap_info_hoteles(url_selenium, sleep_time=5)
    df_hoteles_competencia = pd.DataFrame(dictio_final_hoteles)
    df_hoteles_competencia.to_pickle(archivo_salida_selenium)

    dictio_final_eventos = scrap_info_eventos(url_api)
    df_eventos = pd.DataFrame(dictio_final_eventos) 
    df_eventos.to_pickle(archivo_salida_api)

if __name__ == "__main__":
    scrapeo_total(URL_SELENIUM, URL_API, ARCHIVO_EXTRACCION_HOTELES, ARCHIVO_EXTRACCION_EVENTOS)


