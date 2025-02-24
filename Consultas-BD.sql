-- Cuantos hoteles tiene la base de datos
select count(*) as numero_hoteles
from hoteles;

-- Cuantas reservas se han hecho
select count(*) as numero_reservas 
from reservas;

-- Identifica los 10 clientes que más se han gastado
select 
	concat(c.nombre, ' ', c.apellido) as nombre_cliente,
	sum(r.precio_noche) as gasto_total
from clientes c 
join reservas r on c.id_cliente = r.id_cliente
group by nombre_cliente
order by gasto_total desc
limit 10;

-- Identifica el hotel de la competencia y el hotel de nuestra marca que más han recaudado para esas fechas
select 
	h.nombre_hotel as nombre_hotel,
	sum(r.precio_noche) as recaudacion_total
from hoteles h
join reservas r on h.id_hotel = r.id_hotel
where h.competencia = True
group by nombre_hotel
order by recaudacion_total desc
limit 1;

select 
	h.nombre_hotel as nombre_hotel,
	sum(r.precio_noche) as recaudacion_total
from hoteles h
join reservas r on h.id_hotel = r.id_hotel
where h.competencia = False
group by nombre_hotel
order by recaudacion_total desc
limit 1;

-- Identifica cuantos eventos hay.
select count(*) as num_eventos
from eventos;

-- Identifica el día que más reservas se han hecho para nuestro hoteles
select 
	count(r.fecha_reserva) as recuento_reservas,
	r.fecha_reserva as fecha_reserva
from reservas r 
group by fecha_reserva
order by recuento_reservas desc
limit 1;


