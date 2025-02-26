import pandas as pd 
import numpy as np 
import psycopg2

def conex_bd_creacion_cursor(nombre_db, usuario, contraseña, servidor, puerto):
    """
    Establece una conexión con una base de datos PostgreSQL y crea un cursor para interactuar con la base de datos.

    Args:
        nombre_db (str): El nombre de la base de datos a la que se quiere conectar.
        usuario (str): El nombre de usuario para la conexión a la base de datos.
        contraseña (str): La contraseña del usuario para la conexión.
        servidor (str): La dirección del servidor de la base de datos.
        puerto (int): El puerto en el que la base de datos está escuchando.

    Returns:
        tuple: Un par de objetos:
            - conn (psycopg2.extensions.connection): La conexión a la base de datos.
            - cur (psycopg2.extensions.cursor): El cursor creado para interactuar con la base de datos.
    """
    conn = psycopg2.connect(
    dbname=nombre_db,
    user = usuario,
    password = contraseña,
    host = servidor,
    port = puerto)

    cur = conn.cursor()

    return conn, cur


def insercion_db(conn, cur, insert_query, data_to_insert):
    """
    Ejecuta una inserción masiva de datos en la base de datos utilizando el cursor proporcionado.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        insert_query (str): La consulta SQL para insertar los datos en la base de datos.
        data_to_insert (list): Una lista de tuplas o listas que contienen los datos a insertar en la base de datos.

    Returns:
        None: La función realiza una inserción en la base de datos.
    """
    cur.executemany(insert_query, data_to_insert)
    conn.commit()


def creacion_tabla_ciudad(conn, cur, dataframe):
    """
    Crea una tabla de ciudades en la base de datos a partir de los datos proporcionados en el DataFrame.
    Solo se insertan las ciudades únicas encontradas en el DataFrame.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        dataframe (pd.DataFrame): El DataFrame que contiene la columna `ciudad` con los nombres de las ciudades.

    Returns:
        None: La función inserta las ciudades en la base de datos.
    """
    tabla_ciudad = pd.DataFrame(dataframe["ciudad"].unique(), columns=["nombre_ciudad"])
    data_to_insert = [[row["nombre_ciudad"]] for indice, row in tabla_ciudad.iterrows()]
    insert_query = """
        INSERT INTO ciudad (nombre_ciudad)
        VALUES (%s)
    """

    insercion_db(conn, cur, insert_query, data_to_insert)


def obtener_ciudad_dict(cur):
    """
    Obtiene un diccionario que mapea los nombres de las ciudades a sus respectivos identificadores (`id_ciudad`) 
    a partir de la base de datos.

    Args:
        cur (psycopg2.extensions.cursor): El cursor para ejecutar la consulta.

    Returns:
        dict: Un diccionario con los nombres de las ciudades como claves y sus correspondientes `id_ciudad` como valores.
    """
    cur.execute("SELECT nombre_ciudad, id_ciudad FROM ciudad")
    return dict(cur.fetchall())


def creacion_tabla_eventos(conn, cur, dataframe):
    """
    Crea una tabla de eventos en la base de datos utilizando los datos proporcionados en el DataFrame, 
    relacionando los eventos con las ciudades mediante el `id_ciudad`.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        dataframe (pd.DataFrame): El DataFrame que contiene la información de los eventos a insertar en la base de datos.

    Returns:
        None: La función inserta los eventos en la base de datos.
    """
    ciudad_dict = obtener_ciudad_dict(cur)

    data_to_insert = []
    df_evento = dataframe[["nombre_evento", "url_evento", "codigo_postal", "direccion", "horario", "inicio_evento", "fin_evento", "organizacion", "ciudad"]].drop_duplicates()
    for _, row in df_evento.iterrows(): 
        nombre_evento = row["nombre_evento"]
        url_evento = row["url_evento"]
        codigo_postal = row["codigo_postal"]
        codigo_postal = int(codigo_postal) if pd.notna(codigo_postal) else None
        direccion = row["direccion"]
        horario = row["horario"]
        fecha_inicio = pd.to_datetime(row["inicio_evento"])
        fecha_fin = pd.to_datetime(row["fin_evento"])
        organizacion = row["organizacion"]
        ciudad = row["ciudad"]
        id_ciudad = int(ciudad_dict.get(ciudad))
        data_to_insert.append([nombre_evento, url_evento, codigo_postal, direccion, horario, fecha_inicio, fecha_fin, organizacion, id_ciudad])

    insert_query = """
        INSERT INTO eventos (nombre_evento, url_evento, codigo_postal, direccion, horario, fecha_inicio, fecha_fin, organizacion, id_ciudad)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insercion_db(conn, cur, insert_query, data_to_insert)


def creacion_tabla_hoteles(conn, cur, dataframe1, dataframe2):
    """
    Crea una tabla de hoteles en la base de datos, diferenciando entre hoteles propios y de la competencia, 
    y los asocia a las ciudades correspondientes mediante el `id_ciudad`.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        dataframe1 (pd.DataFrame): El DataFrame que contiene los datos de los hoteles propios.
        dataframe2 (pd.DataFrame): El DataFrame que contiene los datos de los hoteles de la competencia.

    Returns:
        None: La función inserta los hoteles en la base de datos.
    """
    ciudad_dict = obtener_ciudad_dict(cur)

    df_propios = dataframe1[dataframe1["competencia"] == False][["nombre_hotel", "estrellas", "competencia", "ciudad"]].drop_duplicates()
    df_competencia = dataframe2[["nombre_hotel", "estrellas"]].assign(competencia=True, ciudad="Madrid")
    df_hoteles = pd.concat([df_propios, df_competencia], ignore_index=True)
    data_to_insert = []
    df_hotel = df_hoteles[["nombre_hotel", "estrellas", "competencia", "ciudad"]]
    for _, row in df_hotel.iterrows(): 
        nombre_hotel = row["nombre_hotel"]
        estrellas = row["estrellas"]
        competencia = row["competencia"]
        ciudad = row["ciudad"]
        id_ciudad = int(ciudad_dict.get(ciudad))
        data_to_insert.append([nombre_hotel, estrellas, competencia, id_ciudad]) 

    insert_query = """
        INSERT INTO hoteles (nombre_hotel, estrellas, competencia, id_ciudad)
        VALUES (%s, %s, %s, %s)
    """
    insercion_db(conn, cur, insert_query, data_to_insert)


def creacion_tabla_clientes(conn, cur, dataframe):
    """
    Crea una tabla de clientes en la base de datos a partir de los datos proporcionados en el DataFrame, 
    insertando únicamente los clientes únicos.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        dataframe (pd.DataFrame): El DataFrame que contiene la información de los clientes a insertar en la base de datos.

    Returns:
        None: La función inserta los clientes en la base de datos.
    """
    data_to_insert = []
    df_clientes = dataframe[["id_cliente", "nombre", "apellido", "mail"]].drop_duplicates()
    for _, row in df_clientes.iterrows(): 
        id_cliente = row["id_cliente"]
        nombre = row["nombre"]
        apellido = row["apellido"]
        mail = row["mail"]
        data_to_insert.append([id_cliente, nombre, apellido, mail]) 
    
    insert_query = """
        INSERT INTO clientes (id_cliente, nombre, apellido, mail)
        VALUES (%s, %s, %s, %s)
    """
    insercion_db(conn, cur, insert_query, data_to_insert)


def creacion_tabla_reservas(conn, cur, dataframe):
    """
    Crea una tabla de reservas en la base de datos utilizando los datos proporcionados en el DataFrame. 
    Relaciona cada reserva con un cliente y un hotel mediante sus respectivos `id_cliente` e `id_hotel`.

    Args:
        conn (psycopg2.extensions.connection): La conexión a la base de datos.
        cur (psycopg2.extensions.cursor): El cursor para ejecutar las consultas.
        dataframe (pd.DataFrame): El DataFrame que contiene la información de las reservas a insertar en la base de datos.

    Returns:
        None: La función inserta las reservas en la base de datos.
    """
    cur.execute("SELECT nombre_hotel, id_hotel FROM hoteles")
    hotel_dict = dict(cur.fetchall())
    cur.execute("SELECT mail, id_cliente FROM clientes")
    cliente_dict = dict(cur.fetchall())    

    data_to_insert = []
    df_tabla_reservas = dataframe[["id_reserva", "fecha_reserva", "inicio_estancia", "final_estancia", "precio_noche", "mail", "nombre_hotel"]]
    for _, row in df_tabla_reservas.iterrows(): 
        id_reserva = row["id_reserva"]
        fecha_reserva = row["fecha_reserva"]
        inicio_estancia = row["inicio_estancia"]
        final_estancia = row["final_estancia"]
        precio_noche = row["precio_noche"]
        mail = row["mail"]
        id_cliente = cliente_dict.get(mail)
        nombre_hotel = row["nombre_hotel"]
        id_hotel = hotel_dict.get(nombre_hotel)
        data_to_insert.append([id_reserva, fecha_reserva, inicio_estancia, final_estancia, precio_noche, id_cliente, id_hotel]) 
    
    insert_query = """
        INSERT INTO reservas (id_reserva, fecha_reserva, inicio_estancia, final_estancia, precio_noche, id_cliente, id_hotel)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    insercion_db(conn, cur, insert_query, data_to_insert)

def crear_tablas(nombre_db, usuario, contraseña, servidor, puerto, df, df_eventos, df_hoteles_competencia):
    """
    Realiza el proceso completo de creación de las tablas en la base de datos y carga de los datos 
    desde los DataFrames proporcionados.

    Este proceso incluye la creación de las siguientes tablas en la base de datos:
        - Ciudad
        - Eventos
        - Hoteles
        - Clientes
        - Reservas

    Args:
        nombre_db (str): El nombre de la base de datos en la que se realizarán las inserciones.
        usuario (str): El nombre de usuario para la conexión a la base de datos.
        contraseña (str): La contraseña del usuario para la conexión.
        servidor (str): La dirección del servidor donde se encuentra la base de datos.
        puerto (int): El puerto en el que la base de datos está escuchando.
        df (pd.DataFrame): DataFrame con la información de las ciudades, clientes y reservas.
        df_eventos (pd.DataFrame): DataFrame con la información de los eventos.
        df_hoteles_competencia (pd.DataFrame): DataFrame con la información de los hoteles de la competencia.

    Returns:
        None: Esta función no retorna ningún valor, solo realiza las operaciones de inserción en la base de datos.

    Raises:
        Exception: Si ocurre un error en el proceso de conexión, creación de tablas o inserción de datos,
                   se lanzará una excepción con el mensaje correspondiente.
    """
    try:
        # establece conexion y crea cursor
        conn, cur = conex_bd_creacion_cursor(nombre_db, usuario, contraseña, servidor, puerto)

        # crea la tabla de ciudad
        try:
            creacion_tabla_ciudad(conn, cur, df)
        except Exception as e:
            raise Exception(f"Error al crear la tabla de ciudades: {e}")

        # crea la tabla de eventos
        try:
            creacion_tabla_eventos(conn, cur, df_eventos)
        except Exception as e:
            raise Exception(f"Error al crear la tabla de eventos: {e}")

        # crea la tabla de hoteles
        try:
            creacion_tabla_hoteles(conn, cur, df, df_hoteles_competencia)
        except Exception as e:
            raise Exception(f"Error al crear la tabla de hoteles: {e}")

        # crea la tabla de clientes
        try:
            creacion_tabla_clientes(conn, cur, df)
        except Exception as e:
            raise Exception(f"Error al crear la tabla de clientes: {e}")

        # crea la tabla de reservas
        try:
            creacion_tabla_reservas(conn, cur, df)
        except Exception as e:
            raise Exception(f"Error al crear la tabla de reservas: {e}")

    except Exception as e:
        # cierra la conexion y el cursor en caso de error
        if conn:
            conn.close()
        if cur:
            cur.close()
        raise Exception(f"Error en el proceso completo de carga de datos: {e}")
    else:
        # cierra la conexion y el cursor en caso de exito
        cur.close()
        conn.close()
