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
        'BALANCE AS OF DATE' as metric_type,
        balances.dollar_value as metric_value
    
    from spine 

    left outer join balances
        on 
            spine.date_day between 
                balances.active_from and 
                balances.active_to

),

gross_income as (

    select 
        spine.date_day,
        'GROSS INCOME' as metric_name,
        'YEAR TO DATE' as metric_type,
         
	    sum(dollar_value) as metric_value
    
    from spine 

    left outer join transactions
        on 
            spine.date_day >= date_trunc('year', transactions.pc_transaction_date) and 
            spine.date_day >= transactions.pc_transaction_date

    where 
        transactions.contribution_is_transfer = false and 
        transactions.is_income = true

    group by 1

),

savings as (

    select 
        spine.date_day,
        'SAVINGS' as metric_name,
        'YEAR TO DATE' as metric_type,
         
	    sum(dollar_value) as metric_value
    
    from spine 

    left outer join transactions
        on 
            spine.date_day >= date_trunc('year', transactions.pc_transaction_date) and 
            spine.date_day >= transactions.pc_transaction_date

    where transactions.is_savings = true

    group by 1

),

savings_rate as (

    select 
        spine.date_day,
        'SAVINGS RATE' as metric_name,
        'RATE AS OF DATE' as metric_type,

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