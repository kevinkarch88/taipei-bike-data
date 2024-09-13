WITH usage_data AS (
    SELECT
        fact.station_id,
        fact.timestamp,
        fact.free_bikes,
        fact.empty_slots,
        dim.name AS station_name,
        CASE
            WHEN (fact.free_bikes + fact.empty_slots) = 0 THEN 0
            ELSE (fact.empty_slots / (fact.free_bikes + fact.empty_slots)) * 100
        END AS bike_usage_percentage
    FROM
        `taipei-bike-data-project.bike_big_query.fact_table` AS fact
    JOIN
        `taipei-bike-data-project.bike_big_query.dim_table` AS dim
    ON
        fact.station_id = dim.station_id
)
, average_usage AS (
    SELECT
        station_id,
        station_name,
        ROUND(AVG(bike_usage_percentage), 2) AS avg_usage_percentage
    FROM
        usage_data
    GROUP BY
        station_id,
        station_name
)
SELECT
    station_id,
    station_name,
    avg_usage_percentage
FROM
    average_usage
ORDER BY
    avg_usage_percentage DESC
