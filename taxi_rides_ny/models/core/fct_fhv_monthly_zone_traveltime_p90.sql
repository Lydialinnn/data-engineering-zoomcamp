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
        PUlocationID AS pickup_locationid,
        DOlocationID AS dropoff_locationid,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM 
        {{ ref('dim_fhv_trips') }}  -- Reference your 'dim_fhv_trips' model here
    WHERE 
        EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND dispatching_base_num IS NULL
),
p90_trip_data AS (
    SELECT
        year,
        month,
        pickup_locationid,
        dropoff_locationid,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration,
        PERCENTILE_CONT(trip_duration, 0.90) 
        OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid) AS p90_trip_duration
    FROM 
        fhv_trip_data
),
selected_trips AS (
    -- Filter trips starting from Newark Airport, SoHo, and Yorkville East in November 2019
    SELECT 
        p90_trip_data.*,
        pickup_zone.zone AS pickup_zone,
        dropoff_zone.zone AS dropoff_zone
    FROM 
        p90_trip_data
    LEFT JOIN {{ ref('dim_zones') }} AS pickup_zone 
        ON p90_trip_data.pickup_locationid = pickup_zone.locationid
    LEFT JOIN {{ ref('dim_zones') }} AS dropoff_zone 
        ON p90_trip_data.dropoff_locationid = dropoff_zone.locationid
    WHERE 
        p90_trip_data.month = 11
        AND (pickup_zone.zone = 'Newark Airport' OR pickup_zone.zone = 'SoHo' OR pickup_zone.zone = 'Yorkville East')
)

SELECT
    year,
    month,
    pickup_locationid,
    dropoff_locationid,
    dropoff_zone,
    p90_trip_duration,
    ROW_NUMBER() OVER (PARTITION BY year, month, pickup_locationid, dropoff_locationid ORDER BY p90_trip_duration DESC) AS trip_rank
FROM 
    selected_trips
WHERE 
    trip_rank = 2  -- Filter to get the second longest p90 trip duration
