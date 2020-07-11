with 

raw_data as (

    select * from {{ source('raw_google_sheets_deep_work_tracker', 'dw_log') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id'
            ]
        ) }} as tracker_activity_id,
        
        type as activity_type,
        subtype as activity_subtype,
        minutes as activity_minutes,
        date as activity_date,

        _sdc_batched_at as el_batched_at,
        _sdc_extracted_at as el_extracted_at,
        _sdc_received_at as el_recieved_at,
        _sdc_sequence as el_sequence,
        _sdc_table_version as el_table_version
    
    from raw_data

)

select * from cleaned