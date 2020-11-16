with 

raw_data as (

    select * from {{ source('raw', 'okr_objectives')}}

),

cleaned as (

    select 
        objective_id,
        objective_category_id,
        
        objective_text,

        to_date(active_from, 'MM/DD/YYYY') as date_active_from,
        to_date(active_to, 'MM/DD/YYYY') as date_active_to,
        to_date(objective_target_date, 'MM/DD/YYYY') as date_objective_target

    from raw_data 

)

select * from cleaned