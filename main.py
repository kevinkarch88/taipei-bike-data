import logging
from dateutil import parser
import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import sys

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s - %(funcName)s - %(lineno)d', datefmt='%H:%M:%S')

# Spark needs to use the right Python version
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Timeout settings probably not needed but could be if data expands
spark = SparkSession.builder \
    .appName("BikeStationDataProcessing") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

# Log Spark session creation
logging.info("Spark session created successfully.")

# Taipei You-Bike data updated every few seconds
url = "https://api.citybik.es/v2/networks/youbike-new-taipei"

# Perform the GET request and check the response
response = requests.get(url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    data = response.json()
    logging.info("Data fetched successfully.")
elif response.status_code == 400:
    logging.error("Bad Request: The server could not understand the request.")
    sys.exit(1) # Exit on failure
else:
    logging.error(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
    sys.exit(1)  # Exit on failure

# Fact table with changing data
fact_schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("free_bikes", IntegerType(), True),
    StructField("empty_slots", IntegerType(), True)
])

# Dimension table with static data
dim_schema = StructType([
    StructField("id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("district", StringType(), True),
    StructField("address", StringType(), True)
])

fact_data_list = []
dim_data_list = []

for station in data['network']['stations']:
    station_id = station.get("id")

    timestamp_str = station.get("timestamp", datetime.datetime.utcnow().isoformat())
    timestamp = parser.isoparse(timestamp_str) if timestamp_str else datetime.datetime.utcnow()
    # Rounded to nearest minute, don't think we need seconds/microseconds
    timestamp = timestamp.replace(second=0, microsecond=0)

    # We know number of bikes isn't going to be more than 2^8
    free_bikes = max(0, min(station.get("free_bikes", 0), 255))
    empty_slots = max(0, min(station.get("empty_slots", 0), 255))

    fact_data_list.append((
        station_id, timestamp, free_bikes, empty_slots
    ))

    # Don't need more than 4 places for lat/long
    latitude = round(station.get("latitude", 0.0), 4)
    longitude = round(station.get("longitude", 0.0), 4)

    # We're storing addresses/names in English
    extra = station.get("extra", {})
    en_info = extra.get("en", {})

    english_name = en_info.get("name", "").strip()
    english_district = en_info.get("district", "").replace(" Dist", "").strip()
    english_address = en_info.get("address", "").strip()

    dim_data_list.append((
        station_id, latitude, longitude, english_name, english_district, english_address
    ))

logging.info("Bike station data parsed successfully.")

fact_df = spark.createDataFrame(fact_data_list, schema=fact_schema)
dim_df = spark.createDataFrame(dim_data_list, schema=dim_schema)

logging.info("DataFrames created successfully.")

fact_df.show()
dim_df.show()

fact_df.coalesce(1) \
    .write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("fact_bike_availability.csv")

dim_df.coalesce(1) \
    .write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("dim_stations.csv")

logging.info(f"Number of records in fact table: {fact_df.count()}")
logging.info(f"Number of records in dimension table: {dim_df.count()}")

spark.stop()

logging.info("Spark session stopped.")
