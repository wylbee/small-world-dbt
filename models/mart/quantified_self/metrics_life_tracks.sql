with 

spine as (

    select * from {{ ref('day_date_spine') }}

),

tracks as (

    select * from {{ ref('fct_life_tracks') }}

),

atoms as (

    select 
        spine.date_day,
        'NUM_ATOMS' as metric_name,
        'DAILY_COUNT' as metric_type,
        sum(tracker_value) as metric_value

    from spine 

    left outer join tracks 
        on 
            spine.date_day = tracks.date_completed and
            tracker_name = 'atom'

    group by 1

),

unioned as (

    select * from atoms 

)

select * from unioned