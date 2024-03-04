USE default;

-- Set the number of reducers to 1
SET mapreduce.job.reduces = 1;

--- Create external table for airline delay fights
CREATE EXTERNAL TABLE IF NOT EXISTS airline_delay_flights(
     `No` INT,
     `Year` INT,
     `Month` INT,
     `DayofMonth` INT,
     `dayofweek` INT,
     `DepTime` INT,
     `CRSDepTime` INT,
     `ArrTime` INT,
     `CRSArrTime` INT,
     `UniqueCarrier` STRING,
     `FlightNum` STRING,
     `TailNum` STRING,
     `ActualElapsedTime` INT,
     `CRSElapsedTime` INT,
     `AirTime` INT,
     `ArrDelay` INT,
     `DepDelay` INT,
     `Origin` STRING,
     `Dest` STRING,
     `Distance` INT,
     `TaxiIn` INT,
     `TaxiOut` INT,
     `Cancelled` BOOLEAN,
     `CancellationCode` STRING,
     `Diverted` BOOLEAN,
     `CarrierDelay` INT,
     `NASDelay` INT,
     `WeatherDelay` INT,
     `LateAircraftDelay` INT,
     `SecurityDelay` INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

--- Insert data into the table from dataset file
LOAD DATA INPATH '${INPUT}' INTO TABLE airline_delay_flights;

-- Query 01
-- Year wise Carrier delay
-- Record start time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/CarrierDelay/${ITERATION_NO}/timestamps/start_time'
SELECT unix_timestamp(current_timestamp()) as start_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/CarrierDelay/${ITERATION_NO}/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, AVG((CarrierDelay / ArrDelay) * 100) AS CarrierDelay_Percent
FROM airline_delay_flights
GROUP BY Year;

-- Record end time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/CarrierDelay/${ITERATION_NO}/timestamps/end_time'
SELECT unix_timestamp(current_timestamp()) as end_time;

-- Query 02
-- Year wise NAS delay
-- Record start time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/NASDelay/${ITERATION_NO}/timestamps/start_time'
SELECT unix_timestamp(current_timestamp()) as start_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/NASDelay/${ITERATION_NO}/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT Year, AVG((NASDelay / ArrDelay) * 100) AS NASDelay_Percent
FROM airline_delay_flights
GROUP BY Year;

-- Record end time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/NASDelay/${ITERATION_NO}/timestamps/end_time'
SELECT unix_timestamp(current_timestamp()) as end_time;

-- Query 03
-- Year wise Weather delay
-- Record start time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/WeatherDelay/${ITERATION_NO}/timestamps/start_time'
SELECT unix_timestamp(current_timestamp()) as start_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/WeatherDelay/${ITERATION_NO}/output'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
SELECT Year, AVG((WeatherDelay / ArrDelay) * 100) AS WeatherDelay_Percent
FROM airline_delay_flights
GROUP BY Year;

-- Record end time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/WeatherDelay/${ITERATION_NO}/timestamps/end_time'
SELECT unix_timestamp(current_timestamp()) as end_time;

-- Query 04
-- Year wise Late Aircraft delay
-- Record start time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/LateAircraftDelay/${ITERATION_NO}/timestamps/start_time'
SELECT unix_timestamp(current_timestamp()) as start_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/LateAircraftDelay/${ITERATION_NO}/output'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
SELECT Year, AVG((LateAircraftDelay / ArrDelay) * 100) AS LateAircraftDelay_Percent
FROM airline_delay_flights
GROUP BY Year;

-- Record end time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/LateAircraftDelay/${ITERATION_NO}/timestamps/end_time'
SELECT unix_timestamp(current_timestamp()) as end_time;


-- Query 05
-- Year wise Late Security delay
-- Record start time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/SecurityDelay/${ITERATION_NO}/timestamps/start_time'
SELECT unix_timestamp(current_timestamp()) as start_time;

INSERT OVERWRITE DIRECTORY '${OUTPUT}/SecurityDelay/${ITERATION_NO}/output'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
SELECT Year, AVG((SecurityDelay / ArrDelay) * 100) AS SecurityDelay_Percent
FROM airline_delay_flights
GROUP BY Year;

-- Record end time
INSERT OVERWRITE DIRECTORY '${OUTPUT}/SecurityDelay/${ITERATION_NO}/timestamps/end_time'
SELECT unix_timestamp(current_timestamp()) as end_time;
