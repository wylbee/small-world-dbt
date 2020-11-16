with 

raw_data as (

    select * from {{ source('raw','personal_capital_transactions') }}

),

add_row_number as (

    select
        *,
        row_number() over () as row_num
    
    from raw_data

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                '"Date"',
                '"Description"',
                '"Account"',
                '"Amount"',
                '"Category"',
                'row_num'
            ]
        ) }} as pc_transaction_id,
        "Account" as account_name,
        "Amount" :: numeric as dollar_value,
        "Category" as category,
        to_date("Date", 'MM/DD/YYYY') as pc_transaction_date,
        "Description" as pc_transaction_description

    from add_row_number

)

select * from cleaned