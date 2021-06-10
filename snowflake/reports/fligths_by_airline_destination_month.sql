-- Total number of flights by airline and airport on a monthly basis (using destination airport)
create or replace view fligths_by_airline_destination_month as
with aggregation as (
  select
    airline,
    destination_airport,
    year,
    month,
    count(*) as flights
  from flights
  group by airline, destination_airport, year, month
)
select      
    ag.airline as airline_code,
    al.airline as airline_name,   
    ag.destination_airport,
    ap.airport as airport_name,
    ap.city || ', ' || ap.state as airport_location,
    to_char(to_date(ag.year || '-' || ag.month || '-01'), 'YYYY-MM') as month,
    ag.flights
from aggregation ag
left join airlines al on al.iata_code = ag.airline
left join airports ap on ap.iata_code = ag.destination_airport
order by airline_code, destination_airport, year, month;

use user_bkovch;