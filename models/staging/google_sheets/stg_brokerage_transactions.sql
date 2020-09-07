with 

raw_data as (

    select * from {{ source('raw_google_sheets_financial_data', 'm1_transactions') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id'
            ]
        ) }} as brokerage_transaction_id,
        
        "Type" as brokerage_transaction_type,
        "Description" as brokerage_transaction_description,
        "Net Amount" as net_amount,
        "Settle Date" as date_active
    
    from raw_data

)

select * from cleaned