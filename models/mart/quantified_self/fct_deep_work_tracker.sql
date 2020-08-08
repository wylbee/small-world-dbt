{{ config(materialized='table') }}

with 

activities as (

    select * from {{ ref('stg_deep_work_tracker') }}

),

final as (

    select 
        tracker_activity_id,
        activity_type,
        activity_subtype,
        activity_minutes,
        activity_date,
        
        extract('week' from activity_date) as activity_week_number,
        
        case 
            when extract(dow from activity_date) = 0 then 7
            else extract(dow from activity_date) 
        end as activity_weekday
    
    from activities

)

select * from final