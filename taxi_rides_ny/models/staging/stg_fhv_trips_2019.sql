{{ 
    config(
        materialized='table'
    ) 
}}

select 
    *
from {{ source('staging', 'For_Hire_Vehicle_dataset') }}  -- Reference the source for FHV data
where EXTRACT(YEAR FROM pickup_datetime) = 2019
  and dispatching_base_num is null


