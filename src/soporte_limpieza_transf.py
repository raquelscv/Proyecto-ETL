import pandas as pd 
import numpy as np 

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