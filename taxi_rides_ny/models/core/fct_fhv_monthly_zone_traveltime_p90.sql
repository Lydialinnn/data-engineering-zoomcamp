{{ 
    config(
        materialized='table'
    ) 
}}

WITH fhv_trip_data AS (
    SELECT
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM 
        {{ ref('dim_fhv_trips') }} 
    WHERE 
        EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND dispatching_base_num IS not NULL
),

p90_trip_data AS (
    SELECT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        PERCENTILE_CONT(trip_duration, 0.90) 
        OVER (PARTITION BY year, month, pickup_zone, dropoff_zone) AS p90_trip_duration
    FROM 
        fhv_trip_data
    WHERE 
        month = 11
        AND (pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East'))
),

ranked_p90_trips AS (
    SELECT
        year,
        month,
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        ROW_NUMBER() OVER (PARTITION BY year, month, pickup_zone ORDER BY p90_trip_duration DESC) AS trip_rank
    FROM 
        p90_trip_data
)

SELECT *
FROM ranked_p90_trips

