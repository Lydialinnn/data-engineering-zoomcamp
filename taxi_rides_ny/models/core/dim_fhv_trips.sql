{{
    config(
        materialized='table'
    )
}}

with stg_fhv_trips_2019 as (
    select * from {{ ref('stg_fhv_trips_2019') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select stg_fhv_trips_2019.dispatching_base_num,
    stg_fhv_trips_2019.pickup_datetime,
    extract(year from stg_fhv_trips_2019.pickup_datetime) as year,
    extract(month from stg_fhv_trips_2019.pickup_datetime) as month,
    stg_fhv_trips_2019.dropOff_datetime,
    stg_fhv_trips_2019.PUlocationID,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    stg_fhv_trips_2019.DOlocationID,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
    stg_fhv_trips_2019.SR_Flag,
    stg_fhv_trips_2019.Affiliated_base_number
from stg_fhv_trips_2019
inner join dim_zones as pickup_zone
on stg_fhv_trips_2019.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on stg_fhv_trips_2019.DOlocationID = dropoff_zone.locationid


