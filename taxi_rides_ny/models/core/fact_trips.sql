{{ config(materialized='table') }}

with trips_unioned as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
    union all 
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
), 
trips as (
    select 
        t.tripid, 
        t.vendorid, 
        t.service_type,
        t.ratecodeid, 
        t.pickup_locationid, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        t.dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        t.pickup_datetime, 
        t.dropoff_datetime, 
        t.store_and_fwd_flag, 
        t.passenger_count, 
        t.trip_distance, 
        t.trip_type, 
        t.fare_amount, 
        t.extra, 
        t.mta_tax, 
        t.tip_amount, 
        t.tolls_amount, 
        t.ehail_fee, 
        t.improvement_surcharge, 
        t.total_amount, 
        t.payment_type, 
        t.payment_type_description,
        -- Adding new time-based dimensions
        extract(year from t.pickup_datetime) as year,
        extract(quarter from t.pickup_datetime) as quarter,
        format('%d/Q%d', extract(year from t.pickup_datetime), extract(quarter from t.pickup_datetime)) as year_quarter,
        extract(month from t.pickup_datetime) as month
    from trips_unioned t
    inner join dim_zones as pickup_zone
        on t.pickup_locationid = pickup_zone.locationid
    inner join dim_zones as dropoff_zone
        on t.dropoff_locationid = dropoff_zone.locationid
)

select * from trips
