-- Cancellation reasons by airport
create or replace view cancellation_reasons_by_airport as
with aggregation as (
  select
    origin_airport as airport,
    cancellation_reason,
    count(*) num_cancellations
  from flights
  where cancelled = true
  group by origin_airport, cancellation_reason
)
select
    ap.iata_code as airport_code,
    ap.airport as airport_name,
    cr.code || ' - ' || cr.description as cancellation_reason,
    coalesce(ag.num_cancellations, 0) as num_cancellations
from airports ap
cross join cancellation_reasons cr 
left join aggregation ag on 
    ag.airport = ap.iata_code and
    ag.cancellation_reason = cr.code
order by ap.airport, cr.code;

select * from cancellation_reasons_by_airport
order by airport_code, cancellation_reason;