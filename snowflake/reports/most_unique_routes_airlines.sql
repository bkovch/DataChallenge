use user_bkovch;

-- Airlines with the most unique routes
create or replace view most_unique_routes_airlines as
with distinct_routes as (
  -- list all distinct routes per airline
  select distinct
    airline,
    origin_airport,
    destination_airport
  from flights  
), 
unique_routes as (
  -- count the number of unique routes per airline
  -- sort by number of unique routes in descending order
  select
    airline,
    count(*) as routes
  from distinct_routes
  group by airline
  order by routes desc
)
select
    ur.airline as airline_code,
    al.airline as airline_name,
    ur.routes as unique_routes
from airlines al
join unique_routes ur on ur.airline = al.iata_code
order by ur.routes desc;


select * from most_unique_routes_airlines;