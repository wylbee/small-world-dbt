with

dates as (

    select * from {{ ref('day_date_spine') }}

),

books as (

    select * from {{ ref('stg_goodreads') }}

),

targets as (

    select * from {{ ref('tf_kpi_targets') }}

),

classified as (

    select
        books.*,
        'books_read' as task_category
    
    from books

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
        coalesce(count(book_id),0) as daily_books_actual,
        coalesce(max(targets.target_value),0) as daily_books_target
    
    from joined

    left outer join classified
        on 
            joined.date_day = classified.date_read and 
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

        sum(daily_books_actual) over (
            partition by 
                task_category,
                date_part('year', date_day),
                date_part('week', date_day)
            order by date_day
        ) as weekly_books_actual,

        sum(daily_books_target) over (
            partition by 
                task_category,
                date_part('year', date_day),
                date_part('week', date_day)
            order by date_day
        ) as weekly_books_target,

        avg(daily_books_actual) over (
            partition by task_category
            order by date_day rows between (7*6) preceding and current row                
        ) as rolling_avg_daily_books_actual
    
    from aggregated

)

select * from expanded