/*
    According to Bureau of Transportation Statistics:
    A flight is counted as "on time" if it operated less than 15 minutes
    later than the scheduled time shown in the carriers' Computerized
    Reservations Systems (CRS). Arrival performance is based on arrival
    at the gate. Departure performance is based on departure from the
    gate.
    Source: https://www.bts.gov/explore-topics-and-geography/topics/airline-time-performance-and-causes-flight-delays#:~:text=A%20flight%20is%20counted%20as,on%20departure%20from%20the%20gate.

    For the industry an aircraft is on-time when it 
    arrives within 15 minutes of the scheduled arrival time or
    departs within 15 minutes of the scheduled departure time.
    So, exactly 15 minutes after the scheduled time is late.
    Anything up to that is on-time.
    Source: https://www.oag.com/airline-on-time-performance-defining-late
*/

-- On time percentage of each airline for the year 2015
create or replace view on_time_pct_by_airline_2015 as
with total_flights as (
  select 
    airline,
    year,
    count(*) as flights
  from flights
  where arrival_delay is not null -- exclude nulls (canceled flights)
  group by airline, year  
),
on_time_flights as (
  select
    airline,
    year,
    count(*) as flights
  from flights
  where departure_delay < 15 and arrival_delay < 15
  group by airline, year
)
select 
    t.airline,
    al.airline as airline_name,   
    t.flights as total_flights,
    coalesce(ot.flights, 0) as on_time_flights, -- no delays found means 0 delays
    to_number(on_time_flights / total_flights * 100, 5, 2) as on_time_pct
from total_flights t
left join on_time_flights ot on 
    t.airline = ot.airline and
    t.year = ot.year
left join airlines al on al.iata_code = t.airline
where t.year = 2015 -- easy to change in one place, if needed 
order by on_time_pct desc
;
