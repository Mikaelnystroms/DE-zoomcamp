{{
    config(
        materialized='view'
    )
}}

with tripdata as 
(
  select *,
  from {{ source('staging','fhv_data') }}
  where pickup_datetime between '2019-01-01 00:00:00' AND '2019-12-31 23:59:59'
)
select
    {{ dbt.safe_cast("PUlocationID", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("DOlocationID", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    dispatching_base_num,
    SR_Flag,
    Affiliated_base_number
from tripdata
