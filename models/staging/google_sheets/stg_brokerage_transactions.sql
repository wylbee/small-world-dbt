with 

raw_data as (

    select * from {{ source('raw', 'm1_transactions') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'settle_date',
                'description'
            ]
        ) }} as brokerage_transaction_id,
        
        type as brokerage_transaction_type,
        description as brokerage_transaction_description,
        to_number(net_amount,'L9G999g999.99') as dollar_value,
        to_date(settle_date, 'MM/DD/YYYY') as date_active
    
    from raw_data

)

select * from cleaned