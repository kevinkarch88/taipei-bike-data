WITH usage_data AS (
    SELECT
        fact.station_id,
        dim.district,
        dim.name AS station_name,
        fact.timestamp,
        fact.free_bikes,
        fact.empty_slots,
        (fact.free_bikes + fact.empty_slots) AS total_slots
    FROM
        `taipei-bike-data-project.bike_big_query.fact_table` AS fact
    JOIN
        `taipei-bike-data-project.bike_big_query.dim_table` AS dim
    ON
        fact.station_id = dim.station_id
)
, district_variability AS (
    SELECT
        district,
        ROUND(STDDEV(free_bikes), 4) AS bike_availability_stddev,
        ROUND(STDDEV(empty_slots), 4) AS slot_availability_stddev,
        COUNT(DISTINCT station_id) AS station_count
    FROM
        usage_data
    GROUP BY
        district
)
SELECT
    district,
    bike_availability_stddev,
    slot_availability_stddev,
    station_count
FROM
    district_variability
ORDER BY
    bike_availability_stddev DESC
LIMIT 1000
