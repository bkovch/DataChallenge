-- Cancellation reasons by airport (pivoted)
create or replace view cancellation_reasons_by_airport_pivoted as
select
    p.airport_code,
    p.airport_name,
    p."'A - Airline/Carrier'" as airline_carrier,
    p."'B - Weather'" as weather,
    p."'C - National Air System'" as national_air_system,
    p."'D - Security'" as security,
   p."'A - Airline/Carrier'" + p."'B - Weather'" + p."'C - National Air System'" +  p."'D - Security'" as total
from cancellation_reasons_by_airport
pivot (
    min(num_cancellations)
    for cancellation_reason in ('A - Airline/Carrier', 'B - Weather', 'C - National Air System', 'D - Security')
) as p
order by airport_code;


select * from cancellation_reasons_by_airport_pivoted;