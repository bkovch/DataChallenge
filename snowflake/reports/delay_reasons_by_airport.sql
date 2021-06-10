-- Delay reasons by airport
create or replace view delay_reasons_by_airport as
with air_system_delays as(
  select
    origin_airport as airport,
    count(*) as delays 
  from flights
  where air_system_delay > 0
  group by origin_airport
),
security_delays as(
  select
    origin_airport as airport,
    count(*) as delays 
  from flights
  where security_delay > 0
  group by origin_airport
),
airline_delays as(
  select
    origin_airport as airport,
    count(*) as delays 
  from flights
  where airline_delay > 0
  group by origin_airport
),
late_aircraft_delays as(
  select
    origin_airport as airport,
    count(*) as delays 
  from flights
  where late_aircraft_delay > 0
  group by origin_airport
),
weather_delays as(
  select
    origin_airport as airport,
    count(*) as delays 
  from flights
  where weather_delay > 0
  group by origin_airport
)
select
    ap.iata_code as airport_code,
    ap.airport as airport_name,
    ap.city || ', ' || ap.state as city,
    coalesce(asd.delays, 0) as air_system_delays,
    coalesce(sd.delays, 0) as security_delays,
    coalesce(ad.delays, 0) as airline_delays,
    coalesce(lad.delays, 0) as late_aircraft_delays,
    coalesce(wd.delays, 0) as weather_delays
from airports ap
left join air_system_delays asd on asd.airport = ap.iata_code
left join security_delays sd on sd.airport = ap.iata_code
left join airline_delays ad on ad.airport = ap.iata_code
left join late_aircraft_delays lad on lad.airport = ap.iata_code
left join weather_delays wd on wd.airport = ap.iata_code
order by ap.iata_code;
