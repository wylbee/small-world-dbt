with

spine as (

    select * from {{ ref('day_date_spine') }}

),

balances as (

    select * from {{ ref('fct_financial_actuals') }}

),

transactions as (

    select * from {{ ref('fct_financial_transactions') }}

),

balances_joined as (

    select 
        spine.date_day,
        balances.metric_name,
        'BALANCE_AS_OF_DATE' as metric_type,
        balances.dollar_value as metric_value
    
    from spine 

    left outer join balances
        on 
            spine.date_day >= balances.active_from and 
            spine.date_day < balances.active_to

),

gross_income as (

    select 
        spine.date_day,
        'GROSS_INCOME' as metric_name,
        'YEAR_TO_DATE' as metric_type,
         
	    sum(dollar_value) as metric_value
    
    from spine 

    left outer join transactions
        on 
            spine.date_day >= date_trunc('year', transactions.date_active) and 
            spine.date_day >= transactions.date_active

    where 
        transactions.contribution_is_transfer = false and 
        transactions.is_income = true

    group by 1

),

savings as (

    select 
        spine.date_day,
        'SAVINGS' as metric_name,
        'YEAR_TO_DATE' as metric_type,
         
	    sum(dollar_value) as metric_value
    
    from spine 

    left outer join transactions
        on 
            spine.date_day >= date_trunc('year', transactions.date_active) and 
            spine.date_day >= transactions.date_active

    where transactions.is_savings = true

    group by 1

),

savings_rate as (

    select 
        spine.date_day,
        'SAVINGS_RATE' as metric_name,
        'YEAR_TO_DATE' as metric_type,

        savings.metric_value / gross_income.metric_value as metric_value
    
    from spine 

    left outer join savings on spine.date_day = savings.date_day

    left outer join gross_income on spine.date_day = gross_income.date_day

),

unioned as (

    select * from balances_joined

    union all 

    select * from gross_income

    union all 

    select * from savings

    union all 

    select * from savings_rate

)

select * from unioned