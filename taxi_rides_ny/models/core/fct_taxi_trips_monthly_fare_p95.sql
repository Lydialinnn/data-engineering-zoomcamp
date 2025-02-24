{{ 
    config(
        materialized='table'
    ) 
}}

with filtered_trips as (
    select 
        service_type,
        year,
        month,
        fare_amount,
        trip_distance,
        payment_type_description
    from {{ ref('fact_trips') }}  -- Referencing the base fact table
    where year = 2020 and month = 4
      and fare_amount > 0
      and trip_distance > 0
      and payment_type_description in ('Cash', 'Credit Card')
),

percentiles as (
    select
        service_type,
        year,
        month,
        APPROX_QUANTILES(fare_amount, 100) as quantiles
    from filtered_trips
    group by service_type, year, month
)

select
    service_type,
    year,
    month,
    quantiles[OFFSET(97)] as p97,
    quantiles[OFFSET(95)] as p95,
    quantiles[OFFSET(90)] as p90
from percentiles
