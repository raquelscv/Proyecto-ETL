import pandas as pd 
import numpy as np 
import pyarrow as pa

def transf_col_datetime(dataframe, lista_col):
    for col in lista_col:
        dataframe[col] = pd.to_datetime(dataframe[col])
    return dataframe

def relleno_blancos(dataframe, lista_col):
    for col in lista_col:
        dataframe[col] = dataframe[col].replace("", np.nan)
    return dataframe

def nulos_fechas_estancia(dataframe, lista_col):
    for col in lista_col:
        dataframe[col] = dataframe[col].fillna(dataframe[col].dropna().unique()[0])
    return dataframe

def lista_columnas_df(dataframe, columna):
    lista = []  
    for valor in dataframe[columna]:
        lista.append(valor)
    return lista

def creacion_diccionarios(lista1, lista2):
    dicc = dict(zip(lista1, lista2))
    return dicc

def rellenar_nulos_competencia(dataframe, dicc, columna_rellenar, columna_coincidencia):
    dataframe[columna_rellenar] = dataframe.apply(lambda row: dicc.get(row[columna_coincidencia], row[columna_rellenar]), axis=1)

def relleno_nulos_precio_propios(dataframe, dicc):
    for hotel in dicc:
        dataframe.loc[(dataframe["nombre_hotel"] == hotel) & (dataframe["competencia"] == False), "precio_noche"] = dataframe.loc[(dataframe["nombre_hotel"] == hotel) & (dataframe["competencia"] == False), "precio_noche"].fillna(round(dicc[hotel], 2))

def asignar_id_clientes_por_mail(dataframe):
    lista_unicos = dataframe["mail"].unique()  
    dicc_id_clientes = {} 
    conteo = 1  

    for correo in lista_unicos:
        dicc_id_clientes[correo] = conteo
        conteo += 1  

    dataframe["id_cliente"] = dataframe["mail"].map(dicc_id_clientes).astype(str)
    
    return dataframe 

def limpieza_transformacion(archivo_entrada, df_hoteles_competencia, archivo_salida):

    df = pd.read_parquet(archivo_entrada)

    transf_col_datetime(df, ["fecha_reserva", "inicio_estancia", "final_estancia"])

    df.drop_duplicates(inplace=True)

    relleno_blancos(df, ["fecha_reserva", "nombre_hotel", "ciudad"])

    df["ciudad"] = df["ciudad"].fillna("Madrid")

    nulos_fechas_estancia(df, ["inicio_estancia", "final_estancia"])

    df["estrellas"] = df["nombre_hotel"].map(df.groupby("nombre_hotel")["estrellas"].mean().round(1))

    asignar_id_clientes_por_mail(df)

    df_medidas = df.groupby("nombre_hotel")["precio_noche"].describe().reset_index()

    dicc_medidas = creacion_diccionarios(lista_columnas_df(df_medidas, "nombre_hotel"), lista_columnas_df(df_medidas, "mean"))

    relleno_nulos_precio_propios(df, dicc_medidas)

    lista_id_hoteles = df[df["competencia"] == True]["id_hotel"].unique().tolist()

    dicc_id = creacion_diccionarios(lista_id_hoteles, lista_columnas_df(df_hoteles_competencia, "nombre_hotel"))

    rellenar_nulos_competencia(df, dicc_id, "nombre_hotel", "id_hotel")

    dicc_precios = creacion_diccionarios(lista_columnas_df(df_hoteles_competencia, "nombre_hotel"), lista_columnas_df(df_hoteles_competencia, "precio_noche"))

    rellenar_nulos_competencia(df, dicc_precios, "precio_noche", "nombre_hotel")

    dicc_fechas = creacion_diccionarios(lista_columnas_df(df_hoteles_competencia, "nombre_hotel"), lista_columnas_df(df_hoteles_competencia, "fecha_reserva"))

    rellenar_nulos_competencia(df, dicc_fechas, "fecha_reserva", "nombre_hotel")

    df.to_pickle(archivo_salida)

    return df 