-- models/silver/silver_normalized.sql

{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    description="Normalized business data from staging: clean types, deduplicated, enriched"
) }}

with source as (

    select
        id,
        date::date                 as date,     -- normalize to date only
        lower(trim(color))         as color,    -- normalize colors to lowercase, no whitespace
        round(value::numeric, 6)   as value     -- fix precision
    from {{ ref('stg_raw') }}

),

deduplicated as (

    select distinct *
    from source

)

select * from deduplicated
