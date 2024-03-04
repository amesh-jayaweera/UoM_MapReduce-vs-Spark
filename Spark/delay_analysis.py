from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("AirlineDelayAnalysis") \
    .getOrCreate()

# Load CSV data into DataFrame
airline_delay_flights = spark.read.csv("s3://airlinedataset/DelayedFlights-updated.csv", header=True, inferSchema=True)

# Define list of queries
queries = [
    ("CarrierDelay", "CarrierDelay_Percent"),
    ("NASDelay", "NASDelay_Percent"),
    ("WeatherDelay", "WeatherDelay_Percent"),
    ("LateAircraftDelay", "LateAircraftDelay_Percent"),
    ("SecurityDelay", "SecurityDelay_Percent")
]

# Dictionary to store query execution times
query_times = []


# five (5) iterations
for i in range(5):
    # Iterate through queries
    for query_name, result_column in queries:
        # Start time for query execution
        start_time = time.time()

        # Execute query
        result_df = airline_delay_flights.groupBy("Year") \
            .agg(avg((col(query_name) / col("ArrDelay")) * 100).alias(result_column))

        # End time for query execution
        end_time = time.time()

        # Calculate query execution time
        query_time = end_time - start_time

        # Record query execution time
        query_times.append((query_name, i, query_time))

        # Save query result to S3
        result_df.write.option("header", "true").mode("overwrite").csv(f"s3://cs5229ass01s3/spark/output/{i}/{query_name}")

# Convert query times dictionary to DataFrame and save as CSV
query_times_df = spark.createDataFrame(query_times, ["Query Name", "Iteration", "Time Taken"])
query_times_df.write.mode("overwrite").csv("s3://cs5229ass01s3/spark/output/execution_stats")

# Stop Spark session
spark.stop()
