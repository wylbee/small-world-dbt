with

raw_data as (

    select * from {{ source('raw', 'account_balances') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'date_pulled',
                'account_name',
                'asset_name'
            ]
        ) }} as account_balance_id,

        account_name,
        account_type,
        asset_category,
        asset_class,
        asset_name,

        shares,
        to_number(price,'L9G999g999.99') as price,

        to_date(date_pulled, 'MM/DD/YYYY') as date_pulled


    from raw_data 

)

select * from cleaned