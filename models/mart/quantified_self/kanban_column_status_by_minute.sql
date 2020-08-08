{{ config(materialized='table') }}

with 

activity as (

    select * from {{ ref('fct_kanban_activity') }}

),

spine as (

    select * from {{ ref('minute_date_spine') }}

),

filtered as (

    select * from activity 
    
    where 
        column_name is not null and 
        card_name is not null

),

joined as (

    select 
        spine.date_minute,
        filtered.card_id,
        filtered.card_name,
        filtered.column_name
    
    from spine

    inner join filtered
        on 
            spine.date_minute >= filtered.start_date and 
            spine.date_minute <= filtered.end_date

)

select * from joined