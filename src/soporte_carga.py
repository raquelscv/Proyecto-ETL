import pandas as pd 
import numpy as np 
import psycopg2

def conex_bd_creacion_cursor(nombre_db, usuario, contraseña, servidor, puerto):
    conn = psycopg2.connect(
    dbname=nombre_db,
    user = usuario,
    password = contraseña,
    host = servidor,
    port = puerto)

    cur = conn.cursor()

    return conn, cur

def insercion_db(conn, cur, insert_query, data_to_insert):
    cur.executemany(insert_query, data_to_insert)
    conn.commit()

def creacion_tabla_ciudad(conn, cur, dataframe):
    tabla_ciudad = pd.DataFrame(dataframe["ciudad"].unique(), columns=["nombre_ciudad"])
    data_to_insert = [[row["nombre_ciudad"]] for indice, row in tabla_ciudad.iterrows()]
    insert_query = """
        INSERT INTO ciudad (nombre_ciudad)
        VALUES (%s)
    """

    insercion_db(conn, cur, insert_query, data_to_insert)

def obtener_ciudad_dict(cur):
    cur.execute("SELECT nombre_ciudad, id_ciudad FROM ciudad")
    return dict(cur.fetchall())

def creacion_tabla_eventos(conn, cur, dataframe):
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