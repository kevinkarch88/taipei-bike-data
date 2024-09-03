# Taipei YouBike Data Pipeline

This project is a pipeline for processing and analyzing bike station data from the Taipei YouBike system, sourced from the CityBikes API.
The pipeline uses Pyspark for data processing, BigQuery for storage, and DataProc for running Spark jobs, and Composer for orchestration.

# Architecture
Two tables are used to store the data.
The dimension table has the information about each individual station with primary key station_id, longitude, latitude, name, district, and address.
The fact table has the updated status of the bike with primary key station_id, timestamp, free_bikes, and empty_slots.

# Prerequisites

Python 3.8+
Spark, GCP

# Setup


# GCP Next Steps [DRAFT]
Run job (DataProc)
Orchestrate jobs (Composer)
Queries (DBT)