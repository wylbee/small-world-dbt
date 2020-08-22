with

balances as (

    select * from {{ ref('stg_account_balances') }}

),

funds as (

    select * from {{ ref('stg_fund_conversions') }}

),

split as (

    select 
        balances.account_name,
        balances.account_type,
        balances.asset_name,

        coalesce(funds.asset_category, balances.asset_category) as asset_category,

        coalesce(funds.asset_class, balances.asset_class) as asset_class,

        coalesce(
            funds.percent * balances.shares,
            1 * balances.shares
        ) as shares,

        balances.price,

        balances.date_pulled

    from balances

    left outer join funds
        on 
            balances.asset_name = funds.asset_name and 
            balances.date_pulled = funds.date_pulled

),

add_pk_and_value as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'account_name',
                'asset_name',
                'asset_category',
                'asset_class',
                'date_pulled'
            ]
        ) }} as account_categorized_asset_id,
        *,
        
        round(
            shares * price,
            2
         ) as dollar_value

    from split

)

select * from add_pk_and_value