-- Airlines with the largest number of delays
create or replace view delays_by_airline as
with departure_delays as (
  select 
    airline,
    count(*) as delays
  from flights
  where departure_delay > 0
  group by airline 
),
arrival_delays as (
  select 
    airline,
    count(*) as delays
  from flights
  where arrival_delay > 0
  group by airline 
)
select
  al.iata_code as airline_code,
  al.airline as airline_name,
  coalesce(dd.delays, 0) as departure_delays,
  coalesce(ad.delays, 0) as arrival_delays,
  departure_delays + arrival_delays as total_delays
from airlines al
left join departure_delays dd on dd.airline = al.iata_code
left join arrival_delays ad on ad.airline = al.iata_code
order by total_delays desc;




