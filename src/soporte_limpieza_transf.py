import pandas as pd 
import numpy as np 
import pyarrow as pa
import os

def transf_col_datetime(dataframe, lista_col):
    """
    Convierte las columnas de un DataFrame especificadas en una lista a formato de fecha y hora (datetime).

    Args:
        dataframe (pd.DataFrame): El DataFrame en el que se desea realizar la transformación.
        lista_col (list): Una lista de nombres de las columnas que deben ser convertidas a formato datetime.

    Returns:
        pd.DataFrame: El DataFrame con las columnas especificadas convertidas a formato datetime.
    """
    for col in lista_col:
        dataframe[col] = pd.to_datetime(dataframe[col])
    return dataframe

def relleno_blancos(dataframe, lista_col):
    """
    Reemplaza los valores en blanco (cadenas vacías) en las columnas especificadas por NaN.

    Args:
        dataframe (pd.DataFrame): El DataFrame donde se realizarán las modificaciones.
        lista_col (list): Una lista de nombres de columnas en las que se deben reemplazar las cadenas vacías por NaN.

    Returns:
        pd.DataFrame: El DataFrame con los valores vacíos reemplazados por NaN.
    """
    for col in lista_col:
        dataframe[col] = dataframe[col].replace("", np.nan)
    return dataframe

def nulos_fechas_estancia(dataframe, lista_col):
    """
    Rellena los valores nulos en las columnas de fechas de estancia con el valor más frecuente de cada columna (exceptuando los valores NaN).

    Args:
        dataframe (pd.DataFrame): El DataFrame donde se deben rellenar los valores nulos.
        lista_col (list): Una lista de nombres de columnas de fechas en las que se deben rellenar los valores NaN.

    Returns:
        pd.DataFrame: El DataFrame con los valores nulos en las columnas especificadas rellenados.
    """
    for col in lista_col:
        dataframe[col] = dataframe[col].fillna(dataframe[col].dropna().unique()[0])
    return dataframe

def lista_columnas_df(dataframe, columna):
    """
    Convierte los valores de una columna del DataFrame en una lista.

    Args:
        dataframe (pd.DataFrame): El DataFrame del que se extraerá la columna.
        columna (str): El nombre de la columna que se quiere convertir a lista.

    Returns:
        list: Una lista con los valores de la columna especificada del DataFrame.
    """
    lista = []  
    for valor in dataframe[columna]:
        lista.append(valor)
    return lista

def creacion_diccionarios(lista1, lista2):
    """
    Crea un diccionario a partir de dos listas. Los valores de la primera lista se convierten en las claves y los valores de la segunda lista se convierten en los valores del diccionario.

    Args:
        lista1 (list): La lista que se utilizará para las claves del diccionario.
        lista2 (list): La lista que se utilizará para los valores del diccionario.

    Returns:
        dict: Un diccionario donde las claves son los valores de lista1 y los valores son los de lista2.
    """
    dicc = dict(zip(lista1, lista2))
    return dicc

def rellenar_nulos_competencia(dataframe, dicc, columna_rellenar, columna_coincidencia):
    """
    Rellena los valores nulos en una columna específica del DataFrame basándose en un diccionario que mapea valores de otra columna a nuevos valores.

    Args:
        dataframe (pd.DataFrame): El DataFrame en el que se deben rellenar los valores nulos.
        dicc (dict): Un diccionario que mapea valores de la columna de coincidencia a los valores con los que se debe rellenar la columna de interés.
        columna_rellenar (str): El nombre de la columna en la que se deben rellenar los valores nulos.
        columna_coincidencia (str): El nombre de la columna cuyo valor se usará para buscar en el diccionario.

    Returns:
        None: Modifica el dataframe in-place.
    """
    dataframe[columna_rellenar] = dataframe.apply(lambda row: dicc.get(row[columna_coincidencia], row[columna_rellenar]), axis=1)

def relleno_nulos_precio_propios(dataframe, dicc):
    """
    Rellena los valores nulos de la columna precio_noche de los hoteles propios utilizando los precios del diccionario proporcionado.

    Args:
        dataframe (pd.DataFrame): El DataFrame donde se deben rellenar los valores nulos en la columna precio_noche.
        dicc (dict): Un diccionario que contiene los precios de cada hotel para rellenar los valores nulos.

    Returns:
        None: Modifica el dataframe in-place.
    """
    for hotel in dicc:
        dataframe.loc[(dataframe["nombre_hotel"] == hotel) & (dataframe["competencia"] == False), "precio_noche"] = dataframe.loc[(dataframe["nombre_hotel"] == hotel) & (dataframe["competencia"] == False), "precio_noche"].fillna(round(dicc[hotel], 2))

def asignar_id_clientes_por_mail(dataframe):
    """
    Asigna un identificador único (id_cliente) a cada cliente basado en su correo electrónico.

    Args:
        dataframe (pd.DataFrame): El DataFrame que contiene la columna mail para asignar un id_cliente único.

    Returns:
        pd.DataFrame: El DataFrame con la nueva columna id_cliente asignada.
    """
    lista_unicos = dataframe["mail"].unique()  
    dicc_id_clientes = {} 
    conteo = 1  

    for correo in lista_unicos:
        dicc_id_clientes[correo] = conteo
        conteo += 1  

    dataframe["id_cliente"] = dataframe["mail"].map(dicc_id_clientes).astype(str)
    
    return dataframe

def limpieza_transformacion(archivo_entrada, df_hoteles_competencia, archivo_salida):
    """
    Realiza un proceso completo de limpieza y transformación de datos en un DataFrame de reservas de hoteles.

    Esta función realiza diversas tareas de limpieza y transformación de los datos, incluyendo el tratamiento de 
    valores nulos, la conversión de tipos de datos y la creación de nuevas columnas. Además, rellena los valores nulos 
    en función de ciertos criterios basados en el contexto de los hoteles y las reservas. Al final, guarda el DataFrame 
    transformado en un archivo de salida.

    Pasos realizados por la función:
    1. Cargar el archivo de entrada (`archivo_entrada`) en un DataFrame.
    2. Convertir las columnas 'fecha_reserva', 'inicio_estancia' y 'final_estancia' a tipo datetime.
    3. Eliminar duplicados en el DataFrame.
    4. Rellenar los valores vacíos en las columnas 'fecha_reserva', 'nombre_hotel' y 'ciudad' con `NaN`.
    5. Asignar "Madrid" como valor por defecto a los valores nulos de la columna 'ciudad'.
    6. Rellenar valores nulos en las fechas de inicio y final de estancia con el valor de la primera reserva no nula.
    7. Asignar la media de las estrellas por hotel a la columna 'estrellas'.
    8. Asignar un ID único a cada cliente basándose en su correo electrónico ('mail').
    9. Calcular medidas estadísticas descriptivas sobre los precios de las noches por hotel.
    10. Crear un diccionario con la media de precios por hotel.
    11. Rellenar valores nulos en la columna 'precio_noche' de los hoteles propios usando la media de precios por hotel.
    12. Obtener los ID de los hoteles de la competencia.
    13. Crear un diccionario de correspondencia entre el ID de los hoteles y los nombres de los hoteles de la competencia.
    14. Rellenar valores nulos en la columna 'nombre_hotel' de los hoteles de la competencia basándose en el ID de hotel.
    15. Crear un diccionario con los precios de las noches por hotel en la competencia.
    16. Rellenar valores nulos en la columna 'precio_noche' de los hoteles de la competencia con los precios correspondientes.
    17. Crear un diccionario con las fechas de reserva de los hoteles de la competencia.
    18. Rellenar valores nulos en la columna 'fecha_reserva' de los hoteles de la competencia con las fechas correspondientes.
    19. Guardar el DataFrame final en el archivo de salida (`archivo_salida`).

    Args:
        archivo_entrada (str): Ruta al archivo de entrada en formato Parquet que contiene los datos de reservas.
        df_hoteles_competencia (pd.DataFrame): DataFrame con los datos de los hoteles de la competencia.
        archivo_salida (str): Ruta al archivo donde se guardará el DataFrame resultante después de la transformación 
                              (en formato Pickle).

    Returns:
        pd.DataFrame: El DataFrame transformado con las modificaciones aplicadas.

    Raises:
        FileNotFoundError: Si el archivo de entrada no se encuentra en la ruta especificada.
        ValueError: Si el DataFrame de hoteles de la competencia está vacío o si hay un error en la lectura del archivo de entrada.
        KeyError: Si alguna de las columnas necesarias no está presente en el DataFrame.
        FileExistsError: Si el archivo de salida ya existe y no se confirma su sobrescritura.
        RuntimeError: Si ocurre un error durante el proceso de transformación y limpieza de datos.
    """
    # comprueba si el archivo de entrada existe
    if not os.path.exists(archivo_entrada):
        raise FileNotFoundError(f"El archivo de entrada {archivo_entrada} no se encuentra.")

    # comprueba que el DataFrame de competencia no este vacio
    if df_hoteles_competencia.empty:
        raise ValueError("El DataFrame de hoteles de la competencia está vacío.")

    # carga el archivo de entrada (asegurando que es un archivo válido)
    try:
        df = pd.read_parquet(archivo_entrada)
    except Exception as e:
        raise ValueError(f"Error al leer el archivo de entrada: {e}")

    # verifica que las columnas necesarias estan presentes en el DataFrame
    columnas_requeridas = ["fecha_reserva", "inicio_estancia", "final_estancia", "nombre_hotel", "ciudad"]
    for col in columnas_requeridas:
        if col not in df.columns:
            raise KeyError(f"La columna '{col}' no está presente en el DataFrame.")

    try:
        # procede con las transformaciones y limpieza
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

        # verifica si el archivo de salida ya existe y confirmacion de sobreescritura 
        if os.path.exists(archivo_salida):
            overwrite = input(f"El archivo {archivo_salida} ya existe. ¿Desea sobrescribirlo? (s/n): ")
            if overwrite.lower() != 's':
                raise FileExistsError(f"El archivo {archivo_salida} no se ha sobrescrito.")

        df.to_pickle(archivo_salida)
    
    except Exception as e:
        raise RuntimeError(f"Error durante la transformación y limpieza de datos: {e}")

    return df
