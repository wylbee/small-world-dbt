with 

raw_data as (

    select * from {{ source('raw_google_sheets_okrs', 'key_results')}}

),

cleaned as (

    select 
        key_result_id,
        objective_id,
        
        key_result_text,
        key_result_value,
        key_result_unit,
        key_result_metric_name,
        threshold_poor_to_average,
        threshold_average_to_good,
        threshold_good_to_max,

        to_date(active_from, 'YYYY-MM-DD') as date_active_from,
        to_date(active_to, 'YYYY-MM-DD') as date_active_to,
        to_date(key_result_target_date, 'YYYY-MM-DD') as date_key_result_target

    from raw_data 

)

select * from cleaned