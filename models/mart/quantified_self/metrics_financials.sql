with

spine as (

    select * from {{ ref('day_date_spine') }}

),

finance as (

    select * from {{ ref('fct_financial_actuals') }}

),

joined as (

    select 
        spine.date_day,
        finance.metric_name,
        finance.dollar_value as metric_value
    
    from spine 

    left outer join finance
        on 
            spine.date_day between 
                finance.active_from and 
                finance.active_to

)

select * from joined