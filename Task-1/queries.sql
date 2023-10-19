-- setup warehouse, database, and schema
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE AD_FORECAST_DEMO_WH WITH WAREHOUSE_SIZE='XSmall' STATEMENT_TIMEOUT_IN_SECONDS=600 STATEMENT_QUEUED_TIMEOUT_IN_SECONDS=15;
USE WAREHOUSE AD_FORECAST_DEMO_WH;
CREATE DATABASE AD_FORECAST_DEMO;
CREATE SCHEMA AD_FORECAST_DEMO.DEMO;

-- set up analyst role
CREATE ROLE analyst;
GRANT USAGE ON DATABASE AD_FORECAST_DEMO TO ROLE analyst;
GRANT USAGE ON SCHEMA AD_FORECAST_DEMO.DEMO TO ROLE analyst;
GRANT USAGE ON WAREHOUSE AD_FORECAST_DEMO_WH TO ROLE analyst;
GRANT CREATE TABLE ON SCHEMA AD_FORECAST_DEMO.DEMO TO ROLE analyst;
GRANT CREATE VIEW ON SCHEMA AD_FORECAST_DEMO.DEMO TO ROLE analyst;
GRANT CREATE SNOWFLAKE.ML.FORECAST ON SCHEMA AD_FORECAST_DEMO.DEMO TO ROLE analyst;
GRANT CREATE SNOWFLAKE.ML.ANOMALY_DETECTION ON SCHEMA AD_FORECAST_DEMO.DEMO TO ROLE analyst;
GRANT ROLE analyst TO USER <your_user_here>;

-- create table to hold the generated data
create table daily_impressions(day timestamp, impression_count integer);

-- generate random data to fill the table
insert into daily_impressions
select dateadd(day, seq2(1), ('2022-09-01'::timestamp)) as day,
abs(round(normal(35000, 7000, random(4))) as impression_count from table(generator(rowcount=>96));

-- change data to give a slight upward trend and a day of week effect
update daily_impressions
set impression_count=((CASE WHEN dayname(day) IN ('Sat', 'Sun') THEN 0.7
                         WHEN dayname(day)='Fri' THEN 0.9
                         ELSE 1
                     END)*
                  (impression_count+(DATEDIFF(day, '2022-09-01'::timestamp, 
                   day)*120));

-- create the forecast
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST impressions_forecast(INPUT_DATA =>
    SYSTEM$REFERENCE('TABLE', 'daily_impressions'),
                    TIMESTAMP_COLNAME => 'day',
                    TARGET_COLNAME => 'impression_count'
);

-- get the forecast values
CALL impressions_forecast!FORECAST(FORECASTING_PERIODS => 14);

-- select actual data and forecast
SELECT day AS ts, impression_count AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
FROM daily_impressions
UNION ALL
SELECT ts, NULL AS actual, forecast, lower_bound, upper_bound
FROM TABLE(RESULT_SCAN(-1));
