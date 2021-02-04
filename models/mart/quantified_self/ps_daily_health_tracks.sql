with

dates as (

    select * from {{ ref('day_date_spine') }}

),

oura as (

    select * from {{ ref('stg_oura') }}

),

targets as (

    select * from {{ ref('tf_kpi_targets') }}

),

classified as (
    
    select
        oura_id,
        summary_date,
        
        case
            when
                category = 'SLEEP' and
                metric_name = 'score'
                then 'sleep_score'
            when 
                category = 'ACTIVITY' and 
                metric_name = 'score'
                then 'activity_score'
            when 
                category = 'READY' and 
                metric_name = 'score'
                then 'readiness_score'
        end as metric_name,        

        value :: numeric as metric_value
    
    from oura

    where metric_name = 'score'

),

spine as (

    select distinct
        dates.date_day,
        classified.metric_name
    
    from dates

    full outer join classified
        on 1=1

),

aggregated as (

    select
        spine.*,
        coalesce(max(classified.metric_value),0) as daily_value_actual,
        coalesce(max(targets.target_value),0) as daily_value_target
    
    from spine
    
    left outer join classified
        on 
            spine.date_day = classified.summary_date and 
            spine.metric_name = classified.metric_name
    
    left outer join targets
        on
            (spine.date_day between targets.active_from and targets.active_to) and 
            spine.metric_name = targets.target_name

    group by 1,2

),

expanded as (

    select
        *,

        avg(daily_value_actual) over (
            partition by 
                metric_name,
                date_part('year', date_day),
                date_part('week', date_day)
            order by date_day
        ) as avg_weekly_value_actual,

        avg(daily_value_target) over (
            partition by 
                metric_name,
                date_part('year', date_day),
                date_part('week', date_day)
            order by date_day
        ) as avg_weekly_value_target,

        avg(daily_value_actual) over (
            partition by metric_name
            order by date_day rows between (7*6) preceding and current row                
        ) as rolling_avg_daily_value_actual
    
    from aggregated    

)

select * from expanded