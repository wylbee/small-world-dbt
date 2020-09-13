with 

raw_data as (

    select * from {{ source('raw_google_sheets_financial_data', 'brokerage_transactions') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id',
                '__sdc_sheet_id'
            ]
        ) }} as brokerage_transaction_id,
        
        "Type" as brokerage_transaction_type,
        "Description" as brokerage_transaction_description,
        "Net Amount" as dollar_value,
        to_date("Settle Date", 'YYYY-MM-DD') as date_active
    
    from raw_data

)

select * from cleaned