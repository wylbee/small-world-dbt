with

raw_data as (

    select * from {{ source('raw_google_sheets_nomie', 'export') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id',
                '__sdc_sheet_id',
                'tracker',
                'note'
            ]
        ) }} as tracker_id,

        tracker as tracker_name,
        value as tracker_value,
        note as tracker_note,

        split_part(note, '+', 2) as tracker_context,

        to_date("end", 'YYYY-MM-DD') as date_completed
    
    from raw_data

)

select * from cleaned