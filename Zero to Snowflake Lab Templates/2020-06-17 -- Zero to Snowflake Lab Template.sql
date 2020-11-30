// See the original lab PDF guide here - https://s3.amazonaws.com/snowflake-workshop-lab/InpersonZTS_LabGuide.pdf

// This lab has been modified for this audience, so the original lab PDF is slightly different from what we will be running.


//=====================================
// MODULE 1 
// Steps to Prepare Your Lab Environment
//=====================================
/**
 * Make sure you can access {snowflake_account_url}
 *
 * That's it!
 */
//=====================================


//=====================================
// MODULE 2 
// The Snowflake UI & Lab Story
//=====================================
/**
 * 1. Log in to your trial Snowflake instance (check your email for login details)
 * 2. Close any welcome boxes or tutorials
 * 3. UI Tour
 *     - Contexts   - Warehouses
 *     - Databases  - Worksheets
 *     - Shares     - History
 *        
 * Lab Story:
 * This Snowflake lab will be done as part of a theoretical real-world “story” 
 * to help you better understand why we are performing the steps in this lab and 
 * in the order they appear.
 *
 * The “story” of this lab is based on the analytics team at Citi Bike, a real, citywide bike
 * share system in New York City, USA. This team wants to be able to run analytics on
 * data to better understand their riders and how to serve them best.
 *
 * We will first load structured .csv data from rider transactions into Snowflake. 
 * This comes from Citi Bike internal transactional systems. Then later we will 
 * load open-source, semi-structured JSON weather data into Snowflake to see if there is 
 * any correlation between the number of bike rides and weather.
 *        
 */
//=====================================


//=====================================
// MODULE 3
// Preparing to Load Data
//=====================================
// 3.1.1 
USE ROLE        SYSADMIN;
CREATE DATABASE {user}_CITIBIKE;

// 3.1.3 
USE DATABASE  {user}_CITIBIKE;
USE SCHEMA    {user}_CITIBIKE.PUBLIC;

// 3.1.4
CREATE OR REPLACE TABLE 
  {user}_CITIBIKE.PUBLIC.TRIPS (
    TRIPDURATION            INTEGER,
    STARTTIME               TIMESTAMP,
    STOPTIME                TIMESTAMP,
    START_STATION_ID        INTEGER,
    START_STATION_NAME      STRING,
    START_STATION_LATITUDE  FLOAT,
    START_STATION_LONGITUDE FLOAT,
    END_STATION_ID          INTEGER,
    END_STATION_NAME        STRING,
    END_STATION_LATITUDE    FLOAT,
    END_STATION_LONGITUDE   FLOAT,
    BIKEID                  INTEGER,
    MEMBERSHIP_TYPE         STRING,
    USERTYPE                STRING,
    BIRTH_YEAR              INTEGER,
    GENDER                  INTEGER
);

// 3.2.3 | Done in UI
CREATE OR REPLACE STAGE
  {user}_CITIBIKE.PUBLIC.CITIBIKE_TRIPS
  URL='s3://snowflake-workshop-lab/citibike-trips';

// 3.2.4
LIST @CITIBIKE_TRIPS;

// 3.3.2 | Done in UI
CREATE OR REPLACE FILE FORMAT
  {user}_CITIBIKE.PUBLIC.CSV
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'
  NULL_IF = ('NULL','null','')
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;
//=====================================


//=====================================
// MODULE 4
// Loading Data
//=====================================
// 4.1.1 | Done in UI
CREATE OR REPLACE WAREHOUSE
  {user}_COMPUTE_WH
  WAREHOUSE_SIZE=XSMALL
  AUTO_SUSPEND=60
  INITIALLY_SUSPENDED=TRUE;

// 4.2.1 | Done in UI
USE ROLE      SYSADMIN;
USE WAREHOUSE {user}_COMPUTE_WH;
USE DATABASE  {user}_CITIBIKE;
USE SCHEMA    {user}_CITIBIKE.PUBLIC;

// 4.2.2
COPY INTO 
  {user}_CITIBIKE.PUBLIC.TRIPS 
FROM 
  @{user}_CITIBIKE.PUBLIC.CITIBIKE_TRIPS
  FILE_FORMAT={user}_CITIBIKE.PUBLIC.CSV
  ON_ERROR=CONTINUE;

// 4.2.5
TRUNCATE TABLE {user}_CITIBIKE.PUBLIC.TRIPS;

// 4.2.6 | Done in UI
ALTER WAREHOUSE
  {user}_COMPUTE_WH
SET
  WAREHOUSE_SIZE=SMALL;

// 4.2.7
COPY INTO 
  {user}_CITIBIKE.PUBLIC.TRIPS 
FROM 
  @{user}_CITIBIKE.PUBLIC.CITIBIKE_TRIPS
  FILE_FORMAT={user}_CITIBIKE.PUBLIC.CSV
  ON_ERROR=CONTINUE;

// 4.3.1 | Done in UI
CREATE OR REPLACE WAREHOUSE
  {user}_ANALYTICS_WH
  WAREHOUSE_SIZE=XSMALL
  AUTO_SUSPEND=60
  INITIALLY_SUSPENDED=TRUE;
//=====================================


//=====================================
// MODULE 5
// Analytical Queries, Result Cache, Cloning
//=====================================
// 5.1.1 | Done in UI
USE ROLE      SYSADMIN;
USE WAREHOUSE {user}_ANALYTICS_WH;
USE DATABASE  {user}_CITIBIKE;
USE SCHEMA    {user}_CITIBIKE.PUBLIC;

// 5.1.2
SELECT * FROM {user}_CITIBIKE.PUBLIC.TRIPS LIMIT 20;

// 5.1.3
SELECT 
  DATE_TRUNC('hour', STARTTIME) AS DATE,
  COUNT(*)                      AS NUM_TRIPS,
  AVG(TRIPDURATION)/60          AS AVG_DURATION_IN_MINUTES, 
  AVG(
    HAVERSINE(
      START_STATION_LATITUDE, 
      START_STATION_LONGITUDE, 
      END_STATION_LATITUDE, 
      END_STATION_LONGITUDE
    )
  )                             AS AVG_DISTANCE_IN_KILOMETERS 
FROM 
  {user}_CITIBIKE.PUBLIC.TRIPS
GROUP BY 
  DATE 
ORDER BY 
  DATE;

// 5.1.4
SELECT 
  DATE_TRUNC('hour', STARTTIME) AS DATE,
  COUNT(*)                      AS NUM_TRIPS,
  AVG(TRIPDURATION)/60          AS AVG_DURATION_IN_MINUTES, 
  AVG(
    HAVERSINE(
      START_STATION_LATITUDE, 
      START_STATION_LONGITUDE, 
      END_STATION_LATITUDE, 
      END_STATION_LONGITUDE
    )
  )                             AS AVG_DISTANCE_IN_KILOMETERS 
FROM 
  {user}_CITIBIKE.PUBLIC.TRIPS
GROUP BY 
  DATE 
ORDER BY 
  DATE;

// 5.1.5
SELECT
  DAYNAME(STARTTIME) AS WEEKDAY,
  COUNT(*) AS NUM_TRIPS
FROM 
  {user}_CITIBIKE.PUBLIC.TRIPS
GROUP BY 
  WEEKDAY 
ORDER BY 
  NUM_TRIPS DESC;

// 5.2.1
CREATE TABLE 
  {user}_CITIBIKE.PUBLIC.TRIPS_DEV 
CLONE 
  {user}_CITIBIKE.PUBLIC.TRIPS;
//=====================================


//=====================================
// MODULE 6
// Working with Semi-Structured Data, 
// Views, and JOINs
//=====================================
// 6.1.1
USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE {user}_WEATHER;

// 6.1.2
USE ROLE      SYSADMIN;
USE WAREHOUSE {user}_COMPUTE_WH;
USE DATABASE  {user}_WEATHER;
USE SCHEMA    {user}_WEATHER.PUBLIC;

// 6.1.3
CREATE TABLE 
  {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA(
    V VARIANT
  );

// 6.2.1
CREATE STAGE 
  {user}_WEATHER.PUBLIC.NYC_WEATHER
  URL='s3://snowflake-workshop-lab/weather-nyc';

// 6.2.2 
LIST @{user}_WEATHER.PUBLIC.NYC_WEATHER;

// 6.3.1
COPY INTO 
  {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA 
FROM 
  @{user}_WEATHER.PUBLIC.NYC_WEATHER 
  FILE_FORMAT=(
    TYPE=JSON
  );

// 6.3.2
SELECT * FROM {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA LIMIT 10;

// 6.4.1
CREATE VIEW 
  {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA_VIEW 
AS (
  SELECT
    V:"time"::TIMESTAMP                   AS OBSERVATION_TIME,
    V:"city"."id"::INT                    AS CITY_ID,
    V:"city"."name"::STRING               AS CITY_NAME,
    V:"city"."country"::STRING            AS COUNTRY,
    V:"city"."coord"."lat"::FLOAT         AS CITY_LAT,
    V:"city"."coord"."lon"::FLOAT         AS CITY_LON,
    V:"clouds"."all"::INT                 AS CLOUDS,
    (V:"main"."temp"::FLOAT) - 273.15     AS TEMP_AVG,
    (V:"main"."temp_min"::FLOAT) - 273.15 AS TEMP_MIN,
    (V:"main"."temp_max"::FLOAT) - 273.15 AS TEMP_MAX,
    V:"weather"[0]."main"::STRING         AS WEATHER,
    V:"weather"[0]."description"::STRING  AS WEATHER_DESC,
    V:"weather"[0]."icon"::STRING         AS WEATHER_ICON,
    V:"wind"."deg"::FLOAT                 AS WIND_DIR,
    V:"wind"."speed"::FLOAT               AS WIND_SPEED
  FROM 
    {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA
  WHERE 
    CITY_ID = 5128638
);

// 6.4.4
SELECT 
  * 
FROM 
  {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA_VIEW
WHERE 
  DATE_TRUNC('month', OBSERVATION_TIME) = '2018-01-01' 
LIMIT 20;

// 6.5.1
SELECT 
  WEATHER AS CONDITIONS,
  COUNT(*) AS NUM_TRIPS
FROM (
    {user}_CITIBIKE.PUBLIC.TRIPS 
  LEFT OUTER JOIN 
    JSON_WEATHER_DATA_VIEW
  ON 
    DATE_TRUNC('HOUR', OBSERVATION_TIME) = DATE_TRUNC('HOUR', STARTTIME)
)
WHERE 
  CONDITIONS IS NOT NULL
GROUP BY 
  WEATHER 
ORDER BY 
  NUM_TRIPS DESC;
//=====================================


//=====================================
// MODULE 7
// Using Time Travel
//=====================================
// 7.1.1
DROP TABLE {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA;

// 7.1.2
SELECT * FROM JSON_WEATHER_DATA LIMIT 10;

// 7.1.3
UNDROP TABLE {user}_WEATHER.PUBLIC.JSON_WEATHER_DATA;

// 7.2.1
USE ROLE      SYSADMIN;
USE WAREHOUSE {user}_COMPUTE_WH;
USE DATABASE  {user}_CITIBIKE;
USE SCHEMA    {user}_CITIBIKE.PUBLIC;


// 7.2.2
UPDATE 
  {user}_CITIBIKE.PUBLIC.TRIPS 
SET 
  START_STATION_NAME = 'oops';

// 7.2.3
SELECT 
  START_STATION_NAME  AS STATION,
  COUNT(*)            AS RIDES
FROM 
  {user}_CITIBIKE.PUBLIC.TRIPS 
GROUP BY 
  STATION
ORDER BY 
  RIDES DESC
LIMIT 20;


// 7.2.4
SET QUERY_ID = (
  SELECT 
    QUERY_ID 
  FROM 
    TABLE({user}_CITIBIKE.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION (RESULT_LIMIT=>5)) 
  WHERE 
    QUERY_TEXT LIKE 'UPDATE%' 
  ORDER BY 
    START_TIME 
  LIMIT 1
);

SELECT $QUERY_ID; // Check your query id

// 7.2.5
CREATE OR REPLACE TABLE 
  {user}_CITIBIKE.PUBLIC.TRIPS 
AS (
  SELECT 
    * 
  FROM 
    {user}_CITIBIKE.PUBLIC.TRIPS 
  BEFORE 
    (STATEMENT => $QUERY_ID)
);
        
// 7.2.6
SELECT 
  START_STATION_NAME  AS STATION,
  COUNT(*)            AS RIDES
FROM 
  {user}_CITIBIKE.PUBLIC.TRIPS 
GROUP BY 
  STATION
ORDER BY 
  RIDES DESC
LIMIT 20;
//=====================================
          

//=====================================
// MODULE 8
// Roles-Based Access Control (RBAC)
// and Account Administration
//=====================================
// 8.1.1
USE ROLE SECURITYADMIN; 

// 8.1.3 (NOTE - enter your unique user name into the second row below)
CREATE ROLE {user}_JUNIOR_DBA;
GRANT ROLE {user}_JUNIOR_DBA TO USER {user};

// 8.1.4
USE ROLE {user}_JUNIOR_DBA;

// 8.1.6
USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE {user}_CITIBIKE TO ROLE {user}_JUNIOR_DBA;
GRANT USAGE ON DATABASE {user}_WEATHER  TO ROLE {user}_JUNIOR_DBA;

// 8.1.7
USE ROLE {user}_JUNIOR_DBA;
//=====================================
          

//=====================================
// OPTIONAL - Cleanup
//=====================================
USE ROLE SYSADMIN;
DROP DATABASE  IF EXISTS {user}_CITIBIKE;
DROP DATABASE  IF EXISTS {user}_WEATHER;
DROP WAREHOUSE IF EXISTS {user}_ANALYTICS_WH;
DROP WAREHOUSE IF EXISTS {user}_COMPUTE_WH;

USE ROLE SECURITYADMIN;
DROP ROLE IF EXISTS {user}_JUNIOR_DBA;
//=====================================