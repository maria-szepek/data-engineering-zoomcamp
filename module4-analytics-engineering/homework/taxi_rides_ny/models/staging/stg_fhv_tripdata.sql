with source as (
    select * from {{ source('raw', 'fhv_tripdata') }}
),

renamed as (
    select
        -- identifiers
        "dispatching_base_num" as dispatching_base_num,
        cast("PUlocationID" as integer) as pickup_location_id,
        cast("DOlocationID" as integer) as dropoff_location_id,
        "SR_Flag" as sr_flag,
        "Affiliated_base_number" as affiliated_base_number,

        -- timestamps (standardized naming)
        cast("pickup_datetime" as timestamp) as pickup_datetime,
        cast("dropOff_datetime" as timestamp) as dropoff_datetime

    from source
    -- Filter out records with dispatching_base_num null (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

