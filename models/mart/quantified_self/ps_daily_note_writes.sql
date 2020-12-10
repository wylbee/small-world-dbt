with

dates as (

    select * from {{ ref('day_date_spine') }}

),

atoms as (

    select * from {{ ref('stg_zettelkasten') }}

),

targets as (

    select * from {{ ref('tf_kpi_targets') }}

),

classified as (

    select
        atoms.*,
        'atomic_notes' as task_category
    
    from atoms

    where is_first_completion = true

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
        coalesce(count(atom_id),0) as daily_notes_actual,
        coalesce(max(targets.target_value),0) as daily_notes_target
    
    from joined

    left outer join classified
        on 
            joined.date_day = classified.override_dbt_updated_at::timestamp::date and 
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

        sum(daily_notes_actual) over (
            partition by 
                task_category,
                date_part('week', date_day)
            order by date_day
        ) as weekly_notes_actual,

        sum(daily_notes_target) over (
            partition by 
                task_category,
                date_part('week', date_day)
            order by date_day
        ) as weekly_notes_target,

        avg(daily_notes_actual) over (
            partition by task_category
            order by date_day rows between (7*6) preceding and current row                
        ) as rolling_avg_daily_notes_actual
    
    from aggregated

)

select * from expanded