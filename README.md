# Taipei YouBike Data Pipeline

This project is a pipeline for processing and analyzing bike station data from the Taipei YouBike system, sourced from the CityBikes API.
The pipeline uses Pyspark for data processing, Dagster for scheduling, and Docker for containerization.

# Architecture [DRAFT]
Two tables are used to store the data.
The dimension table has the information about each individual station with primary key station_id, longitude, latitude, name, district, and address.
The fact table has the updated status of the bike with primary key station_id, timestamp, free_bikes, and empty_slots.

# Prerequisites

Python 3.8+
Docker
Spark, Dagster, and Dagit (installed with pip)

# Setup
In a terminal, run 'dagit -f main.py'. In a separate terminal run 'dagster-daemon run -f main.py'.
In a third terminal, run 'dagster schedule start' to start the pipeline, and you should see new data created every minute!

# Docker
To build the image, do 'docker build -t taipei-bike-data .'
To run the container, do 'docker run -p 3000:3000 taipei-bike-data'
