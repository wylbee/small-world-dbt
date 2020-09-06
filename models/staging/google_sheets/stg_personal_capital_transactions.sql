with 

raw_data as (

    select * from {{ source('raw_google_sheets_financial_data','personal_capital_transactions') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id',
                '__sdc_sheet_id'
            ]
        ) }} as pc_transaction_id,
        "Account" as account_name,
        "Amount" as dollar_value,
        "Category" as category,
        "Date" as pc_transaction_date,
        "Description" as pc_transaction_description

    from raw_data

)

select * from cleaned