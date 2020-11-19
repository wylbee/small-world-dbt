with

okrs as (

    select * from {{ ref('dim_okrs') }}

),

finance as (

    select * from {{ ref('metrics_financials') }}

),

life as (

    select * from {{ ref('metrics_life_tracks') }}

),

type_adjustements as (

    select * from {{ ref('okr_metric_type_adjustments') }}

),

unioned as (

    select * from finance 

    union

    select * from life

),

adjust_type as (

    select 
        unioned.date_day,
        unioned.metric_name,
        coalesce(type_adjustements.override_type, unioned.metric_type) as metric_type,
        unioned.metric_value

    from unioned

    left outer join type_adjustements
        on unioned.metric_name = type_adjustements.metric_name

),

joined as (

    select 
        adjust_type.date_day,
        adjust_type.metric_name,
        
        case 
            when adjust_type.metric_type like '%AVG_DAILY%' then 
                avg(adjust_type.metric_value) over (
                    partition by adjust_type.metric_name
                    order by adjust_type.date_day
                )
            when adjust_type.metric_type like '%AVG_WEEKLY%' then 
                avg(adjust_type.metric_value) over (
                    partition by adjust_type.metric_name 
                    order by date_part('week', adjust_type.date_day)
                ) * 7
            when adjust_type.metric_type like '%RUNNING_TOTAL%' then 
                sum(adjust_type.metric_value) over (
                    partition by adjust_type.metric_name 
                    order by adjust_type.date_day
                )
            else adjust_type.metric_value
        end as metric_value,

        okrs.key_result_id,
        okrs.key_result_value,

        case
            when 
                metric_type like '%AS_OF_DATE%' or
                metric_type like '%RUNNING_TOTAL%' 
                then 
                    round(
                        okrs.key_result_value::decimal *    (
                            (
                                adjust_type.date_day::date - okrs.date_active_from_key_result::date
                            )::decimal / (
                                okrs.date_active_to_key_result::date - okrs.date_active_from_key_result::date
                            )::decimal
                        ),
                        2
                    ) 
            else round(okrs.key_result_value::decimal, 2)
        end as target_value,

         okrs.threshold_poor_to_average,
         okrs.threshold_average_to_good,
         okrs.threshold_good_to_max
    
    from adjust_type
    
    inner join okrs 
        on 
            adjust_type.metric_name = okrs.key_result_metric_name and 
            adjust_type.date_day between
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