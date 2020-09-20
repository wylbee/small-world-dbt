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

    select distinct
        spine.date_day,
        mapped.metric_name,
        mapped.metric_type

    from spine 

    full outer join mapped
        on 1=1

),

grouped as (

    select 
        joined.*,
        coalesce(sum(mapped.tracker_value/mapped.conversion_denominator),0) as metric_value 
    
    from joined

    left outer join mapped 
        on 
            joined.date_day = mapped.date_completed and 
            joined.metric_name = mapped.metric_name and 
            joined.metric_type = mapped.metric_type
    
    group by 1,2,3

)

select * from grouped