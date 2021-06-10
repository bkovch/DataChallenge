create or replace table cancellation_reasons (
	code varchar(1) not null,
	description varchar(50) not null,
    primary key (code)
) comment='Reasons for Cancellation of flight'
;

insert into cancellation_reasons
values
    ('A', 'Airline/Carrier'),
    ('B', 'Weather'),
    ('C', 'National Air System'),
    ('D', 'Security');

select * from cancellation_reasons;