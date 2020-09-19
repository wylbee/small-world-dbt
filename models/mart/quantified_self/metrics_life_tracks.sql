with 

spine as (

    select * from {{ ref('day_date_spine') }}

),

tracks as (

    select * from {{ ref('fct_life_tracks') }}

),

mapping as (

    select * from {{ ref('life_tracking_mapping') }}

),

mapped as (

    select 
        tracks.*,
        mapping.*
    
    from tracks 

    left outer join mapping 
        on 
            tracks.tracker_name = mapping.tracker_name and 
            tracks.tracker_context = mapping.tracker_context
    
    where mapping.metric_name is not null 

),

joined as (

    select 
        spine.date_day,
        mapped.metric_name,
        mapped.metric_type,
        sum(mapped.tracker_value/mapped.conversion_denominator) as metric_value

    from spine 

    left outer join mapped
        on spine.date_day = mapped.date_completed 

    group by 1,2,3

),

unioned as (

    select * from joined

)

select * from unioned