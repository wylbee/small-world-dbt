with

raw_data as (

    select * from {{ source('raw_google_sheets_financial_data', 'account_balances') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '__sdc_row',
                '__sdc_spreadsheet_id',
                '__sdc_sheet_id'
            ]
        ) }} as account_balance_id,

        account_name,
        account_type,
        asset_category,
        asset_class,
        asset_name,

        shares,
        price,

        to_date(date_pulled, 'YYYY-MM-DD') as date_pulled

    from raw_data 

)

select * from cleaned