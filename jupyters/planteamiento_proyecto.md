# Descripción del Proyecto 

Nuestra empresa se dedica al sector hotelero en Madrid. Se nos ha proporcionado un archivo en formato Parquet que contiene información sobre reservas de hoteles, incluyendo datos de hoteles propios y de la competencia. Nuestro objetivo es extraer, transformar y cargar (ETL) estos datos para generar insights relevantes.

# Creación de la BD 

CREATE TABLE ciudad (
    id_ciudad SERIAL PRIMARY KEY,
    nombre_ciudad TEXT
);
CREATE TABLE eventos (
    id_evento SERIAL PRIMARY KEY,
    nombre_evento TEXT,
    url_evento TEXT,
    codigo_postal INT,
    direccion TEXT,
    horario TEXT,
    fecha_inicio DATE,
    fecha_fin DATE,
    organizacion TEXT,
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE
);
CREATE TABLE hoteles (
    id_hotel SERIAL PRIMARY KEY, -- Puede ser VARCHAR(5)
    nombre_hotel TEXT,
    competencia BOOL,
    estrellas FLOAT CHECK (valoracion BETWEEN 1 AND 5),
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE
);
CREATE TABLE clientes (
    id_cliente VARCHAR(50) PRIMARY KEY,
    nombre TEXT,
    apellido TEXT,
    mail TEXT UNIQUE CHECK (mail LIKE '%@%')
);
CREATE TABLE reservas (
    id_reserva VARCHAR(50) PRIMARY KEY,
    fecha_reserva DATE,
    inicio_estancia DATE,
    final_estancia DATE,
    precio_noche FLOAT CHECK (precio_noche >= 0),
    id_cliente VARCHAR(50) REFERENCES clientes(id_cliente) ON DELETE CASCADE,
    id_hotel INT REFERENCES hoteles(id_hotel) ON DELETE CASCADE
    
);