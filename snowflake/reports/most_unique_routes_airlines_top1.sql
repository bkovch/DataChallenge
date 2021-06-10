-- Airline with the most unique routes (Top 1)
create or replace view most_unique_routes_airlines_top1 as
select top 1 *
from most_unique_routes_airlines
order by unique_routes desc;


select * from most_unique_routes_airlines_top1;