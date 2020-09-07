with

pc as (

    select * from {{ ref('stg_personal_capital_transactions') }}

),

brokerage as (

    select * from {{ ref('stg_brokerage_transactions') }}

),

pc_standardized as (

    select 
        account_name,
        dollar_value,
        category,
        pc_transaction_date as date_active,
        pc_transaction_description as transaction_description

    from pc

),

brokerage_standardized as (

    select 
        'M1 Finance' as account_name,
        dollar_value,
        'Securities Trades' as category,
        date_active,
        brokerage_transaction_description as transaction_description
    
    from brokerage


),

unioned as (

    select * from pc_standardized

    union all 

    select * from brokerage_standardized

)

select * from unioned