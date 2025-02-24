import pandas as pd
import numpy as np
import pyarrow as pa
import os 
from dotenv import load_dotenv
from soporte_limpieza_transf import transf_col_datetime, relleno_blancos, nulos_fechas_estancia, asignar_id_clientes_por_mail, creacion_diccionarios, lista_columnas_df, relleno_nulos_precio_propios, rellenar_nulos_competencia
from src.soporte_excrapeo import scrap_info_hoteles, scrap_info_eventos

load_dotenv()
ARCHIVO_RAW = os.getenv("ARCHIVO_RAW")
ARCHIVO_SALIDA = os.getenv("ARCHIVO_SALIDA")
URL_SELENIUM = os.getenv("URL_SELENIUM")
URL_API = os.getenv("URL_API")
ARCHIVO_EXTRACCION_HOTELES = os.getenv("ARCHIVO_EXTRACCION_HOTELES")
ARCHIVO_EXTRACCION_EVENTOS = os.getenv("ARCHIVO_EXTRACCION_EVENTOS")

def extraccion_transf_carga(archivo_entrada, archivo_salida, url_selenium, url_api, archivo_salida_selenium, archivo_salida_api):

    df_raw = pd.read_parquet(archivo_entrada)

    df = df_raw.copy()

    df.drop_duplicates(inplace=True)
    
    df["ciudad"] = df["ciudad"].fillna("Madrid")

    transf_col_datetime(df, ["fecha_reserva", "inicio_estancia", "final_estancia"])

    relleno_blancos(df, ["fecha_reserva", "nombre_hotel", "ciudad"])

    nulos_fechas_estancia(df, ["inicio_estancia", "final_estancia"])

    df["estrellas"] = df["nombre_hotel"].map(df.groupby("nombre_hotel")["estrellas"].mean().round(1))

    asignar_id_clientes_por_mail(df)

    df_medidas = df.groupby("nombre_hotel")["precio_noche"].describe().reset_index()

    dicc_medidas = creacion_diccionarios(lista_columnas_df(df_medidas, "nombre_hotel"), lista_columnas_df(df_medidas, "mean"))

    relleno_nulos_precio_propios(df, dicc_medidas)

    dictio_final_hoteles = scrap_info_hoteles(url_selenium, sleep_time=5)
    df_hoteles_competencia = pd.DataFrame(dictio_final_hoteles)
    df_hoteles_competencia.to_pickle(archivo_salida_selenium)

    dictio_final_eventos = scrap_info_eventos(url_api)
    df_eventos = pd.DataFrame(dictio_final_eventos) 
    df_eventos.to_pickle(archivo_salida_api)

    lista_id_hoteles = df[df["competencia"] == True]["id_hotel"].unique().tolist()

    dicc_id = creacion_diccionarios(lista_id_hoteles, lista_columnas_df(df_hoteles_competencia, "nombre_hotel"))

    rellenar_nulos_competencia(df, dicc_id, "nombre_hotel", "id_hotel")

    dicc_precios = creacion_diccionarios(lista_columnas_df(df_hoteles_competencia, "nombre_hotel"), lista_columnas_df(df_hoteles_competencia, "precio_noche"))

    rellenar_nulos_competencia(df, dicc_precios, "precio_noche", "nombre_hotel")

    dicc_fechas = creacion_diccionarios(lista_columnas_df(df_hoteles_competencia, "nombre_hotel"), lista_columnas_df(df_hoteles_competencia, "fecha_reserva"))

    rellenar_nulos_competencia(df, dicc_fechas, "fecha_reserva", "nombre_hotel")

    df.to_pickle(archivo_salida)


extraccion_transf_carga(ARCHIVO_RAW, ARCHIVO_SALIDA, URL_SELENIUM, URL_API, ARCHIVO_EXTRACCION_HOTELES, ARCHIVO_EXTRACCION_EVENTOS)

