{{ config(
    tags=["failing"],
    materialized='view',
    description="Failing model"
) }}

with source as (

    select
        id,
        foobar, -- fails here
    from {{ source('raw', 'test') }}

),

select * from source
