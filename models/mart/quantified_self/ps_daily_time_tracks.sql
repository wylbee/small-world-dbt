with

dates as (

    select * from {{ ref('day_date_spine') }}

),

tracks as (

    select * from {{ ref('tf_time_tracks') }}

),

tasks as (

    select * from {{ ref('dim_tasks') }}

),

targets as (

    select * from {{ ref('tf_kpi_targets') }}

),

classified as (

    select
        tracks.*,

        case
            when 
                tasks.is_deep_work and 
                tasks.is_professional
                then 'deep_work_professional'
            when 
                tasks.is_deep_work and
                tasks.is_okr 
                then 'deep_work_okr'
            when tasks.is_slope_learning then 'slope_learning'
        end as task_category
    
    from tracks

    left outer join tasks
        on tracks.task_id = tasks.task_id

),

joined as (

    select distinct
        dates.date_day,
        classified.task_category
    
    from dates

    full outer join classified 
        on 1=1

),

aggregated as (

    select 
        joined.*,
        coalesce(sum(classified.duration_seconds/60.0),0) as daily_minutes_actual,
        coalesce(max(targets.target_value),0) as daily_minutes_target
    
    from joined

    left outer join classified
        on 
            joined.date_day = classified.date_ended::timestamp::date and 
            joined.task_category = classified.task_category
    
    left outer join targets
        on
            (joined.date_day between targets.active_from and targets.active_to) and 
            joined.task_category = targets.target_name
            

    group by 1,2

),

expanded as (

    select 
        *,

        sum(daily_minutes_actual) over (
            partition by 
                task_category,
                date_part('week', date_day)
            order by date_day
        ) as weekly_minutes_actual,

        sum(daily_minutes_target) over (
            partition by 
                task_category,
                date_part('week', date_day)
            order by date_day
        ) as weekly_minutes_target,

        avg(daily_minutes_actual) over (
            partition by task_category
            order by date_day rows between (7*8) preceding and current row                
        ) as rolling_avg_daily_minutes_actual
    
    from aggregated

)

select * from expanded