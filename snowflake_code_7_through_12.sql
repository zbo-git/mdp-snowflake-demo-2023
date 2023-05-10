--set role context
use role sysadmin;

--create new database weather
create database weather;

--set context
use warehouse compute_wh;
use database weather;
use schema public;

--alternatively:
--use weather.public;

--create new table to store JSON data
create table json_weather_data (v variant);

--create new stage for weather data
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

--show files in nyc_weather stage
list @nyc_weather;

--load data from stage into table
--note we don't have to define a custom file format because the JSON is well formed
copy into json_weather_data
from @nyc_weather 
    file_format = (type = json strip_outer_array = true);

--see the data we just loaded
select * from json_weather_data limit 10;

--create a view that will put structure onto the semi-structured data
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';

--verify view
select 
    * 
from 
    json_weather_data_view
where 
    date_trunc('month',observation_time) = '2018-01-01'
limit 20;

--join back to trips to see how weather impacts it
select 
    weather_conditions as conditions
    ,count(*) as num_trips
from 
    citibike.public.trips
    left outer join json_weather_data_view on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where 
    conditions is not null
group by 
    1 
order by 
    2 desc;


--Time Travel
--drop table, verify it's gone, then use Time Travel to restore it
drop table json_weather_data;

select * from json_weather_data limit 10;

undrop table json_weather_data;

--verify table is undropped
select * from json_weather_data limit 10;

--making updates and rolling them back
use database citibike;
use schema public;
update trips set start_station_name = 'oops';

--oops
select
    start_station_name as "station",
    count(*) as "rides"
from 
    trips
group by 
    1
order by 
    2 desc
limit 20;

--get query id of query that updated records
--we will use this query id in Time Travel
--check out the syntax for selecting from table function
set query_id =
(
    select 
        query_id 
    from 
        table(information_schema.query_history_by_session (result_limit=>5))
    where 
        query_text like 'update%' 
    order by 
        start_time desc 
    limit 1
);

--use Time Travel to restore table to how it was before update query ran
create or replace table trips as
(select * from trips before (statement => $query_id));

--verify results
select
    start_station_name as "station",
    count(*) as "rides"
from 
    trips
group by 
    1
order by 
    2 desc
limit 20;



--creating new roles and users
use role accountadmin;

create role junior_dba;

grant role junior_dba to user admin;

--switch to new role
use role junior_dba;
--notice the context has changed

--grant junior_dba access to warehouse and databases and verify they can be accessed
use role accountadmin;

grant usage on warehouse compute_wh to role junior_dba;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;

use role junior_dba;


--creating a share
use role accountadmin;

create or replace share ZERO_TO_SNOWFLAKE_SHARED_DATA;

grant usage on database citibike to share ZERO_TO_SNOWFLAKE_SHARED_DATA;
grant usage on schema citibike.public to share ZERO_TO_SNOWFLAKE_SHARED_DATA;

grant select on all tables in schema public to share ZERO_TO_SNOWFLAKE_SHARED_DATA;

--resetting environment
use role accountadmin;

drop share if exists zero_to_snowflake_shared_data;
-- If necessary, replace "zero_to_snowflake-shared_data" with the name you used for the share
drop database if exists citibike;
drop database if exists weather;
drop warehouse if exists analytics_wh;
drop role if exists junior_dba;