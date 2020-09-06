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