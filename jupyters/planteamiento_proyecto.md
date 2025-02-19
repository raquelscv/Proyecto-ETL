# Descripción del Proyecto 

Nuestra empresa se dedica al sector hotelero. Se nos ha proporcionado un archivo en formato Parquet que contiene información sobre reservas de hoteles, incluyendo datos de hoteles propios y de la competencia. Nuestro objetivo es extraer, transformar y cargar (ETL) estos datos para generar insights relevantes.

# Posible estructuración en la creación de la BBDD 

CREATE TABLE "clientes" (
    "id_cliente" VARCHAR(255) PRIMARY KEY,
    "nombre" VARCHAR(255),
    "apellido" VARCHAR(255),
    "mail" VARCHAR(255)
);

CREATE TABLE "hoteles" (
    "id_hotel" SERIAL PRIMARY KEY,
    "nombre_hotel" VARCHAR(255),
    "estrellas" INT,
    "competencia" BOOLEAN,
    "ciudad" VARCHAR (225)
);

CREATE TABLE "reservas" (
    "id_reserva" VARCHAR(255) PRIMARY KEY,
    "fecha_reserva" DATE,
    "inicio_estancia" DATE, 
    "final_estancia" DATE,
    "id_cliente" INT REFERENCES "clientes"("id_cliente"),
    "id_hotel" INT REFERENCES "hoteles"("id_hotel"), 
    "precio_noche" DECIMAL (10, 2)
);


otra tabla: eventos. un hotel puede estar cerca de varios eventos y ahi entre eventos y hoteles hay una relacion many to many. entonces se necesita una tabla intermedia (ciudad) de donde se saca la relacion 1-1 (la ciudad madrid es lo q tienen en comun los eventos y los hoteles)


Esta podría ser una opción en la estructuración de tablas con sus respectivas columnas a partir de las columnas de las que dispongo inicialmente (id_reserva, id_cliente, nombre, apellido, mail, competencia, fecha_reserva, inicio_estancia, final_estancia, id_hotel, precio_noche, nombre_hotel, estrellas, ciudad).

La tabla principal, es decir, la tabla de hechos sería la tabla de Reservas. Por otro lado, tendríamos las dimensiones que serían las tablas de Clientes y Hoteles que complementarían la información de las Reservas. Por ejemplo, la tabla de Clientes cuenta con la información exclusivamente relacionada con los clientes y la tabla de Hoteles con la información exclusivamente relacionada con los hoteles. 



Para un análisis posterior la creación de determinadas columnas podría resultar interesante:
- En cuanto a la fecha_reserva parece que el año (2025) y el mes (febrero) es el mismo en todos los registros por lo que quizás es más conveniente únicamente disponer de día_reserva. 
- Categorizar la de precio_noche y hacer rangos (precio bajo, medio, alto)

en una carga de base de datos aunque tengas columnas que son iguales para todos los registros no se cargan. en un analisis si que te puedes plantear eliminar esas columnas si para tu analisis no te aportan valor


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
    organizacion TEXT,
    id_ciudad INT REFERENCES ciudad(id_ciudad) ON DELETE CASCADE
);
CREATE TABLE hoteles (
    id_hotel SERIAL PRIMARY KEY,
    nombre_hotel TEXT,
    estrellas INT CHECK (estrellas BETWEEN 1 AND 5),
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