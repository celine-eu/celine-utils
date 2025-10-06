-- models/gold/gold_color_metrics.sql

{{ config(
    materialized='table',
    description="Business-ready metrics aggregated by color and date"
) }}

with source as (

    select
        id,
        date,
        color,
        value
    from {{ ref('silver_normalized') }}

),

aggregated as (

    select
        date,
        color,
        count(distinct id)          as records,
        avg(value)                  as avg_value,
        min(value)                  as min_value,
        max(value)                  as max_value
    from source
    group by date, color

)

select * from aggregated
order by date, color
