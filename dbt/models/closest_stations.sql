WITH station_data AS (
    SELECT
        station_id,
        name AS station_name,
        latitude,
        longitude,
        ST_DISTANCE(
            ST_GEOGPOINT(longitude, latitude),
            ST_GEOGPOINT(121.53219, 25.03379)
        ) AS distance_from_restaurant
    FROM
        `taipei-bike-data-project.bike_big_query.dim_table`
)
SELECT
    station_id,
    station_name,
    latitude,
    longitude,
    ROUND(distance_from_restaurant, 2) AS distance_in_meters
FROM
    station_data
ORDER BY
    distance_from_restaurant
LIMIT 10
