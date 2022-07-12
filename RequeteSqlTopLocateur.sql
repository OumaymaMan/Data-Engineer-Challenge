select client, cus.last_name, cus.first_name, NbreLocation, adr.postal_code, longitude, latitude from 
(
	SELECT customer_id as client, count(*) as NbreLocation
	from rental
	group by customer_id
    order by NbreLocation desc
) AS t 
	NATURAL JOIN customer AS cus
    NATURAL JOIN address AS adr
LIMIT 1;
