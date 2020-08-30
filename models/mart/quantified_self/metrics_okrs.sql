with

okrs as (

    select * from {{ ref('dim_okrs') }}

),

finance as (

    select * from {{ ref('metrics_financials') }}

),

merp as (

    select 
        finance.date_day,
        finance.metric_name,
        finance.metric_value,
        okrs.key_result_value,

        round(
            okrs.key_result_value::decimal *    (
                (
                    finance.date_day::date - okrs.date_active_from_key_result::date
                )::decimal / (
                    okrs.date_active_to_key_result::date - okrs.date_active_from_key_result::date
                )::decimal
            ),
            2
         ) as target_value
    
    from finance
    
    inner join okrs 
        on 
            finance.metric_name = okrs.key_result_metric_name and 
            finance.date_day between
                okrs.date_active_from_key_result and 
                okrs.date_active_to_key_result

)

select * from merp