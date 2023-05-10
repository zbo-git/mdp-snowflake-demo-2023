--set role context
use role sysadmin;

--create database citibike if it doesn't exist, overwrite it if it does
create or replace database citibike;

--alternate syntax - create or replace - doesn't overwrite existing database
--create database if not exists citibike;

--set database and schema context
use database citibike;
use schema public;

--alternatively:
--use citibike.public;

--create table trips. replace it if exists already
--note field data types provided here
create or replace table trips
(
    tripduration integer,
    starttime timestamp,
    stoptime timestamp,
    start_station_id integer,
    start_station_name string,
    start_station_latitude float,
    start_station_longitude float,
    end_station_id integer,
    end_station_name string,
    end_station_latitude float,
    end_station_longitude float,
    bikeid integer,
    membership_type string,
    usertype string,
    birth_year integer,
    gender integer
);

--describe table we just created
--note the data types here have been converted per Snowflake's standards
desc table public.trips;

--create stage pointed to external S3 bucket
create stage citibike_trips
    url = 's3://snowflake-workshop-lab/citibike-trips-csv/';

--list contents of stage we just created
list @citibike_trips;

--create file format
--we will use this file format to ingest files from stage
create or replace file format csv 
    type='csv' --file format
    compression = 'auto' --file compression type
    field_delimiter = ',' --field delimiter character
    record_delimiter = '\n' --record delimiter character
    skip_header = 0 --number of header rows to skip
    field_optionally_enclosed_by = '\042' --character used to enclose strings
    trim_space = false --remove leading/trailing whitespace
    error_on_column_count_mismatch = false --generate error if number of columns in file doesn't match number of columns in table being loaded
    escape = 'none' --escape character
    escape_unenclosed_field = '\134' --escape character for unenclosed field values only
    date_format = 'auto' --date format
    timestamp_format = 'auto' --timestamp format
    null_if = ('') --string used to convert to and from SQL NULL
    comment = 'file format for ingesting data for zero to snowflake' --object comment
;

--verify file format is created

show file formats in database citibike;

--show warehouses, none available to sysadmin
show warehouses;

--switch to accountadmin and grant usage to sysadmin
use role accountadmin;
grant usage on warehouse compute_wh to role sysadmin;
grant modify on warehouse compute_wh to role sysadmin;

--switch back to sysadmin, check if warehouse is available
use role sysadmin;
show warehouses;

alter warehouse compute_wh set warehouse_size=SMALL;

--check warehouse size
show warehouses;


--start to load data
--check context (role, database, schema) first
use role sysadmin;
use database citibike;
use schema public;

--alternatively:
--use citibike.public;

--load data from stage into trips table
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ; --34s

truncate table trips;
--verify table is clear
select * from trips limit 10;

--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';

--load data with large warehouse
show warehouses;

--load same data again
copy into trips from @citibike_trips file_format=csv PATTERN = '.*csv.*' ; --12s

--change warehouse size from large to xsmall
alter warehouse compute_wh set warehouse_size='xsmall';


--create new warehouse analytics_wh
--size = large
create or replace warehouse analytics_wh warehouse_size='large';

--set context
use role sysadmin;
use warehouse analytics_wh;
use citibike.public;

--alternatively:
--use database citibike;
--use schema public;

select * from trips limit 20;

--basic query on this dataset:
select 
    date_trunc('hour', starttime) as "date",
    count(*) as "num trips",
    avg(tripduration)/60 as "avg duration (mins)",
    avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from 
    trips
group by 
    1 
order by 
    1;

--run the same query again
--notice query runtime and stats versus previous run of same query
select 
    date_trunc('hour', starttime) as "date",
    count(*) as "num trips",
    avg(tripduration)/60 as "avg duration (mins)",
    avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from 
    trips
group by 
    1 
order by 
    1;

--find the busiest months
select
    monthname(starttime) as "month",
    count(*) as "num trips"
from 
    trips
group by 
    1 
order by 
    2 desc;

--zero copy cloning
create table trips_dev clone trips;
