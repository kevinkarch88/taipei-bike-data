from dagster import op, job, schedule, in_process_executor, Definitions
import logging
from dateutil import parser
import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s - %(funcName)s - %(lineno)d', datefmt='%H:%M:%S')

# Spark needs to use the right Python version
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


@op
def fetch_bike_data(context):
    url = "https://api.citybik.es/v2/networks/youbike-new-taipei"
    response = requests.get(url)
    if response.status_code == 200:
        context.log.info("Data fetched successfully.")
        return response.json()
    else:
        context.log.error(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return None

# Get fact table with slots and check if new stations
@op
def process_bike_data(context, data, cached_dim_data):
    spark = SparkSession.builder.appName("BikeStationDataProcessing").getOrCreate()

    fact_schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("free_bikes", IntegerType(), True),
        StructField("empty_slots", IntegerType(), True)
    ])

    dim_schema = StructType([
        StructField("id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("name", StringType(), True),
        StructField("district", StringType(), True),
        StructField("address", StringType(), True)
    ])

    fact_data_list = []
    new_dim_data_list = []

    for station in data['network']['stations']:
        station_id = station.get("id")

        # Process fact table data
        timestamp_str = station.get("timestamp", datetime.datetime.utcnow().isoformat())
        timestamp = parser.isoparse(timestamp_str) if timestamp_str else datetime.datetime.utcnow()
        timestamp = timestamp.replace(second=0, microsecond=0)

        free_bikes = max(0, min(station.get("free_bikes", 0), 255))
        empty_slots = max(0, min(station.get("empty_slots", 0), 255))

        fact_data_list.append((station_id, timestamp, free_bikes, empty_slots))

        # Process dimension table only if station_id is new
        if station_id not in cached_dim_data:
            latitude = round(station.get("latitude", 0.0), 4)
            longitude = round(station.get("longitude", 0.0), 4)
            extra = station.get("extra", {})
            en_info = extra.get("en", {})
            english_name = en_info.get("name", "").strip()
            english_district = en_info.get("district", "").replace(" Dist", "").strip()
            english_address = en_info.get("address", "").strip()

            new_dim_data_list.append((
                station_id, latitude, longitude, english_name, english_district, english_address
            ))

            # Log the new station
            context.log.info(f"New station found: {english_name} at {datetime.datetime.utcnow()}")

    # Convert lists to DataFrames
    fact_df = spark.createDataFrame(fact_data_list, schema=fact_schema)
    if new_dim_data_list:
        dim_df = spark.createDataFrame(new_dim_data_list, schema=dim_schema)

        # Save dimension DataFrame to CSV
        dim_filename = f"dim_stations_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        dim_df.coalesce(1) \
            .write.format("csv") \
            .option("header", "true") \
            .mode("overwrite") \
            .save(dim_filename)
        context.log.info(f"Dimension table CSV saved successfully as {dim_filename}")

    # Save fact DataFrame to CSV
    fact_filename = f"fact_bike_availability_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    fact_df.coalesce(1) \
        .write.format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(fact_filename)
    context.log.info(f"Fact table CSV saved successfully as {fact_filename}")

    return fact_filename


# Load existing dimension data from CSV, WIP csv name
@op
def load_cached_dim_data(context):
    if os.path.exists("dim_stations.csv"):
        spark = SparkSession.builder.appName("BikeStationDataProcessing").getOrCreate()
        dim_df = spark.read.format("csv").option("header", "true").load("dim_stations.csv")
        cached_dim_data = set(dim_df.select("id").rdd.flatMap(lambda x: x).collect())
        context.log.info(f"Loaded {len(cached_dim_data)} station IDs from the dimension table.")
    else:
        cached_dim_data = set()
        context.log.info("No existing dimension data found. Starting fresh.")

    return cached_dim_data


# Steps of execution for dagster
@job(executor_def=in_process_executor)
def bike_pipeline():
    data = fetch_bike_data()
    cached_dim_data = load_cached_dim_data()
    process_bike_data(data, cached_dim_data)


# Schedule pipeline to run every minute
@schedule(cron_schedule="*/1 * * * *", job=bike_pipeline, execution_timezone="UTC")
def minute_schedule(_context):
    return {}


# definitions needed to get scheduler to work in UI
defs = Definitions(
    jobs=[bike_pipeline],
    schedules=[minute_schedule]
)


if __name__ == "__main__":
    result = bike_pipeline.execute_in_process()
