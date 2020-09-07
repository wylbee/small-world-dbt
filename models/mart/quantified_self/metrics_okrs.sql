with

okrs as (

    select * from {{ ref('dim_okrs') }}

),

finance as (

    select * from {{ ref('metrics_financials') }}

),

joined as (

    select 
        finance.date_day,
        finance.metric_name,
        finance.metric_value,
        okrs.key_result_id,
        okrs.key_result_value,

        case
            when metric_type like '%AS_OF_DATE%' then 
                round(
                    okrs.key_result_value::decimal *    (
                        (
                            finance.date_day::date - okrs.      date_active_from_key_result::date
                        )::decimal / (
                            okrs.date_active_to_key_result::date - okrs.        date_active_from_key_result::date
                        )::decimal
                    ),
                    2
                ) 
            else okrs.key_result_value 
        end as target_value,

         okrs.threshold_poor_to_average,
         okrs.threshold_average_to_good,
         okrs.threshold_good_to_max
    
    from finance
    
    inner join okrs 
        on 
            finance.metric_name = okrs.key_result_metric_name and 
            finance.date_day between
                okrs.date_active_from_key_result and 
                okrs.date_active_to_key_result

),

add_range as (

    select 
        date_day,
        metric_name,
        metric_value,
        key_result_id,
        key_result_value,

        threshold_poor_to_average * target_value as target_value_poor_to_average,
        threshold_average_to_good * target_value as target_value_average_to_good,
        threshold_good_to_max * target_value as target_value_good_to_max
    
    from joined

)

select * from add_range