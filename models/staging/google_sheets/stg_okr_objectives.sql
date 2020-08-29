with 

raw_data as (

    select * from {{ source('raw_google_sheets_okrs', 'objectives')}}

),

cleaned as (

    select 
        objective_id,
        objective_category_id,
        
        objective_text,

        to_date(active_from, 'YYYY-MM-DD') as date_active_from,
        to_date(active_to, 'YYYY-MM-DD') as date_active_to,
        to_date(objective_target_date, 'YYYY-MM-DD') as date_objective_target

    from raw_data 

)

select * from cleaned