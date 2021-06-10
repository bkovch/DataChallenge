use user_bkovch;

-- Total number of flights by airline and airport on a monthly basis (using both airports)
create or replace view fligths_by_airline_airport_month as
with origin as (
  -- flights from airport
  select
    airline,
    origin_airport,
    year,
    month,
    count(*) as flights
  from flights
  group by airline, origin_airport, year, month
),
destination as (
  -- flights to airport
  select
    airline,
    destination_airport,
    year,
    month,
    count(*) as flights
  from flights
  group by airline, destination_airport, year, month
),
origin_destination as (
  -- flights "from airport" and "to airpot" joined together
  select      
      coalesce(o.airline, d.airline) as airline,
      coalesce(o.origin_airport, d.destination_airport) as airport,
      coalesce(o.year, d.year) as year,
      coalesce(o.month, d.month) as month,
      coalesce(o.flights, 0) as flights_from_airport,
      coalesce(d.flights, 0) as flights_to_airport
  from origin o
  full outer join destination d on
      o.airline = d.airline and
      o.origin_airport = d.destination_airport and
      o.year = d.year and
      o.month = d.month
)
select
    od.airline as airline_code,
    al.airline as airline_name,   
    od.airport as airport,
    ap.airport as airport_name,
    ap.city || ', ' || ap.state as airport_location,
    to_char(to_date(od.year || '-' || od.month || '-01'), 'YYYY-MM') as month,
    od.flights_from_airport,
    od.flights_to_airport
from origin_destination od
left join airlines al on al.iata_code = od.airline
left join airports ap on ap.iata_code = od.airport
order by airline_code, od.airport, od.year, od.month