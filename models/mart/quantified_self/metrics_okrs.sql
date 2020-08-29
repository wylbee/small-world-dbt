with

spine as (

    select * from {{ ref('day_date_spine') }}

),

okrs as (

    select * from {{ ref('dim_okrs') }}

),

finance as (

    select * from {{ ref('fct_financial_actuals') }}

),

merp as (

    select 
        spine.date_day,
        finance.metric_name,
        finance.dollar_value as metric_value,
        okrs.key_result_value,


        round(
            okrs.key_result_value::decimal *    (
                (
                    spine.date_day::date - okrs.date_active_from_key_result::date
                )::decimal / (
                    okrs.date_active_to_key_result::date - okrs.date_active_from_key_result::date
                )::decimal
            ),
            2
         ) as target_value
    
    from spine 

    left outer join finance
        on 
            spine.date_day between 
                finance.active_from and 
                finance.active_to
    
    left outer join okrs 
        on 
            okrs.key_result_id = 2 and 
            spine.date_day between
                okrs.date_active_from_key_result and 
                okrs.date_active_to_key_result

)

select * from merp