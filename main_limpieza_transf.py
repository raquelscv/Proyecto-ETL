import pandas as pd
import numpy as np
import pyarrow as pa
import os 
from dotenv import load_dotenv
from soporte_limpieza_transf import transf_col_datetime, relleno_blancos, nulos_fechas_estancia, asignar_id_clientes_por_mail, creacion_diccionarios, lista_columnas_df, relleno_nulos_precio_propios

load_dotenv()
ARCHIVO_RAW = os.getenv("ARCHIVO_RAW")
ARCHIVO_SALIDA = os.getenv("ARCHIVO_SALIDA")

def limpieza_transf_datos(archivo_entrada, archivo_salida):

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



    df.to_pickle(archivo_salida)


limpieza_transf_datos(ARCHIVO_RAW, ARCHIVO_SALIDA)