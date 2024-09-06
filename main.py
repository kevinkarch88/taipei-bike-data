import logging
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dateutil import parser
import datetime
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import os
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s - %(funcName)s - %(lineno)d', datefmt='%H:%M:%S')

bq_client = bigquery.Client()


def create_or_update_table(table_id, schema):
    try:
        bq_client.get_table(table_id)  # Check if the table exists
        logging.info(f"Table {table_id} already exists.")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)
        bq_client.create_table(table)
        logging.info(f"Table {table_id} created.")


def create_bigquery_schema():
    fact_schema = [
        bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("free_bikes", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("empty_slots", "INTEGER", mode="REQUIRED"),
    ]

    dim_schema = [
        bigquery.SchemaField("station_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("district", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("address", "STRING", mode="REQUIRED"),
    ]

    return fact_schema, dim_schema


def fetch_bike_data():
    url = "https://api.citybik.es/v2/networks/youbike-new-taipei"
    response = requests.get(url)
    if response.status_code == 200:
        logging.info("Data fetched successfully.")
        return response.json()
    else:
        logging.error(f"Failed to fetch data. HTTP Status Code: {response.status_code}")
        return None


def validate_schema(df_schema, table_schema):
    if set(df_schema) != set(table_schema):
        logging.error("Schema mismatch in data frames.")
        return False
    return True


def create_or_update_dataset(dataset_id):
    try:
        bq_client.get_dataset(dataset_id)
        logging.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        bq_client.create_dataset(dataset, timeout=30)
        logging.info(f"Dataset {dataset_id} created.")


def create_spark_schema():
    fact_schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("free_bikes", IntegerType(), False),
        StructField("empty_slots", IntegerType(), False)
    ])

    dim_schema = StructType([
        StructField("station_id", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("name", StringType(), False),
        StructField("district", StringType(), False),
        StructField("address", StringType(), False)
    ])

    return fact_schema, dim_schema


def fetch_existing_station_ids(dataset_id):
    query = f"SELECT station_id FROM `{dataset_id}.dim_table`"

    query_job = bq_client.query(query)
    results = query_job.result()

    station_ids = {row['station_id'] for row in results}

    logging.info(f"Fetched {len(station_ids)} station IDs from BigQuery.")
    return station_ids


def process_bike_data(data, dataset_id):
    spark = SparkSession.builder \
        .appName("BikeStationDataProcessing") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.30.0") \
        .getOrCreate()

    # Create Spark schemas
    spark_fact_schema, spark_dim_schema = create_spark_schema()

    # Create BigQuery schemas
    fact_schema, dim_schema = create_bigquery_schema()

    # Create or update the BigQuery tables
    create_or_update_table(f"{dataset_id}.fact_table", fact_schema)
    create_or_update_table(f"{dataset_id}.dim_table", dim_schema)

    fact_data_list = []
    dim_data_list = []

    existing_station_ids = fetch_existing_station_ids(dataset_id)

    for station in data['network']['stations']:
        station_id = station.get("id")

        # Fact table data
        timestamp_str = station.get("timestamp", datetime.datetime.utcnow().isoformat())
        timestamp = parser.isoparse(timestamp_str) if timestamp_str else datetime.datetime.utcnow()
        timestamp = timestamp.replace(second=0, microsecond=0)

        free_bikes = station.get("free_bikes", 0)
        empty_slots = station.get("empty_slots", 0)

        fact_data_list.append((station_id, timestamp, free_bikes, empty_slots))

        if station_id not in existing_station_ids:
            latitude = round(station.get("latitude", 0.0), 4)
            longitude = round(station.get("longitude", 0.0), 4)
            extra = station.get("extra", {})
            en_info = extra.get("en", {})
            english_name = en_info.get("name", "").strip()
            english_district = en_info.get("district", "").replace(" Dist", "").strip()
            english_address = en_info.get("address", "").strip()

            dim_data_list.append((
                station_id, latitude, longitude, english_name, english_district, english_address
            ))

            logging.info(f"New station found: {english_name}")

    # Convert lists to Spark DataFrames using the Spark schema
    fact_df = spark.createDataFrame(fact_data_list, schema=spark_fact_schema)
    dim_df = spark.createDataFrame(dim_data_list, schema=spark_dim_schema) if dim_data_list else None

    # Fetch the schema from BigQuery to validate it with DataFrame
    table = bq_client.get_table(f"{dataset_id}.fact_table")
    table_schema = [field.name for field in table.schema]

    if validate_schema(fact_df.schema.names, table_schema):
        fact_df.write \
            .format("bigquery") \
            .option("table", f"{dataset_id}.fact_table") \
            .option("writeMethod", "direct") \
            .option("autodetect", "true") \
            .mode("append") \
            .save()

        logging.info(f"Inserted {fact_df.count()} rows into {dataset_id}.fact_table.")

    if dim_df:
        dim_df.write \
            .format("bigquery") \
            .option("table", f"{dataset_id}.dim_table") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()

        logging.info(f"Inserted {dim_df.count()} rows into {dataset_id}.dim_table.")
    else:
        logging.info(f"No new rows inserted into {dataset_id}.dim_table.")


def main():
    dataset_id = "taipei-bike-data-project.bike_big_query"

    create_or_update_dataset(dataset_id)

    # Fetch and process the data
    data = fetch_bike_data()
    if data:
        process_bike_data(data, dataset_id)


if __name__ == "__main__":
    main()
