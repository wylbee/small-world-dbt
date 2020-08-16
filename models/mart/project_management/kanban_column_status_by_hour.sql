{{ config(materialized='table') }}

with 

activity as (

    select * from {{ ref('fct_kanban_activity') }}

),

spine as (

    select * from {{ ref('hour_date_spine') }}

),

filtered as (

    select * from activity 
    
    where 
        column_name is not null and 
        card_name is not null and 
        column_name != '[Resources]'

),

joined as (

    select 
        spine.date_hour,
        filtered.card_id,
        filtered.card_name,
        filtered.column_name,
        filtered.hierarchy
    
    from spine

    inner join filtered
        on 
            spine.date_hour >= filtered.start_date and 
            spine.date_hour <= filtered.end_date

)

select * from joined