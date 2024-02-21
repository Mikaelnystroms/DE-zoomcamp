{{ config(materialized="table") }}

with
    fhv_tripdata as (
        select *
        from {{ ref("stg_fhv_tripdata") }}
        where pickup_locationid is not null and dropoff_locationid is not null
    ),

    dim_zones as (select * from {{ ref("dim_zones") }} where borough != 'Unknown')

select
    cast(fhv_tripdata.pickup_datetime as timestamp) as pickup_datetime,
    cast(fhv_tripdata.dropoff_datetime as timestamp) as dropoff_datetime,
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone

from fhv_tripdata
inner join
    dim_zones as pickup_zone on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join
    dim_zones as dropoff_zone
    on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid
