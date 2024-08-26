# Taipei YouBike Data Pipeline

This project is a pipeline for processing and analyzing bike station data from the Taipei YouBike system, sourced from the CityBikes API.

# Architecture [DRAFT]
PySpark is used to fetch the data, clean it, and arrange it into dimension and fact tables.

Dagster is used to schedule the jobs to run once every minute.
