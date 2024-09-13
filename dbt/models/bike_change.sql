WITH bike_change AS (
    SELECT
        fact.station_id,
        dim.name AS station_name,
        fact.timestamp,
        fact.free_bikes,
        LAG(fact.free_bikes) OVER (PARTITION BY fact.station_id ORDER BY fact.timestamp) AS previous_free_bikes,
        (fact.free_bikes - LAG(fact.free_bikes) OVER (PARTITION BY fact.station_id ORDER BY fact.timestamp)) AS change_in_free_bikes
    FROM
        `taipei-bike-data-project.bike_big_query.fact_table` AS fact
    JOIN
        `taipei-bike-data-project.bike_big_query.dim_table` AS dim
    ON
        fact.station_id = dim.station_id
)
, average_change AS (
    SELECT
        station_id,
        station_name,
        ROUND(AVG(ABS(change_in_free_bikes)), 2) AS avg_change_in_free_bikes
    FROM
        bike_change
    WHERE
        previous_free_bikes IS NOT NULL
    GROUP BY
        station_id,
        station_name
)
SELECT
    station_id,
    station_name,
    avg_change_in_free_bikes
FROM
    average_change
ORDER BY
    avg_change_in_free_bikes DESC
