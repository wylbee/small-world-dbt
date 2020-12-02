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
        coalesce(sum(classified.duration_seconds/60.0),0) as daily_minutes
    
    from joined

    left outer join classified
        on 
            joined.date_day = classified.date_ended::timestamp::date and 
            joined.task_category = classified.task_category
    
    group by 1,2

),

expanded as (

    select 
        *,

        sum(daily_minutes) over (
            partition by 
                task_category,
                date_part('week', date_day)
            order by date_day
        ) as weekly_minutes,

        avg(daily_minutes) over (
            partition by task_category
            order by date_day rows between (7*8) preceding and current row                
        ) as rolling_avg_daily_minutes
    
    from aggregated

)

select * from expanded