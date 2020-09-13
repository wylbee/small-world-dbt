with 

balances as (

    select * from {{ ref('account_balances__split') }}

),

emergency_fund as (

    select 
        'EMERGENCY_FUND' as metric_name,
        dollar_value,

        date_pulled as active_from,
        lag(date_pulled,1,'2099-12-31') over (
            order by date_pulled desc
        ) as active_to
    
    from balances

    where account_name = 'Ally Emergency'

),

union_all as (

    select * from emergency_fund

),

add_pk as (

    select
        {{ dbt_utils.surrogate_key(
            [
                'active_from',
                'metric_name'
            ]
        ) }} as financial_actuals_id,
        *

    from union_all

)

select * from add_pk