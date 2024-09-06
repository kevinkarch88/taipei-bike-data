# Taipei YouBike Data Pipeline

This project is a pipeline for processing and analyzing bike station data from the Taipei YouBike system, sourced from the CityBikes API.
The pipeline uses Pyspark for data processing, BigQuery for storage, DataProc for running Spark jobs, Scheduler for orchestration, and dbt for transformations.

# Architecture
Two tables are used to store the data.
The dimension table has the information about each individual station with primary key station_id, longitude, latitude, name, district, and address.
The fact table has the updated status of the bike with primary key station_id, timestamp, free_bikes, and empty_slots.

# Prerequisites

Python 3.8+
Spark, GCP

# Setup [DRAFT]
Make sure the requirements file has all the packages you need. Install them with:
pip3 install -r requirements.txt
Note: Make sure to use 'pip3' in gcloud or you might just install the dependencies for Python2 and waste an hour trying to figure out what went wrong.

Create a bucket and put your files in there.
gsutil mb gs://taipei-bike-bucket/
gsutil cp main.py gs://taipei-bike-bucket/

Create your dataproc cluster (min size is now 30gb)
gcloud dataproc clusters create my-cluster \
    --region=us-central1 \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=30GB \
    --image-version=2.0-debian10 \
    --project=taipei-bike-data-project


# GCP Next Steps [DRAFT]
Run job (DataProc)
Orchestrate jobs (Scheduler)
Queries & Transformations (DBT)
