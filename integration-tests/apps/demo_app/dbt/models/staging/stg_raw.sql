-- models/staging/stg_raw.sql

{{ config(
    materialized='view',
    description="Staging view of datasets.raw exposing only the business columns"
) }}

with source as (

    select
        id::int                       as id,
        date::timestamp               as date,
        color::text                   as color,
        value::numeric                as value
    from {{ source('raw', 'test') }}

),

final as (

    select
        id,
        date,
        color,
        value
    from source

)

select * from final
