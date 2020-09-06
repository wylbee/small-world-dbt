with

transactions as (

    select * from {{ ref('financial_transactions__unioned') }}

),

mapping as (

    select * from {{ ref('financial_transaction_sr_mapping') }}

),

joined as (

    select 
        transactions.*,
        
        case 
            when category not in ('Transfers', 'Credit Card Payments', 'General Rebalance') and 
            dollar_value > 0 then true
            else false 
        end as is_income,

        mapping.is_savings,
        
        case 
            when 
                mapping.contribution_is_transfer = true and 
                transactions.category in ('Retirement Contributions', 'Securities Trades')
                then true 
            else false 
        end as contribution_is_transfer

    from transactions

    left outer join mapping 
        on transactions.account_name = mapping.account_name

)

select * from joined