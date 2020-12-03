with

targets as (

    select * from {{ ref('kpi_targets') }}

),

cleaned as (

    select
        target_id,
        target_name,
        target_value,
        target_unit,
        to_date(coalesce(active_from::text,'1899-01-01'), 'YYYY-MM-DD') as active_from,
        to_date(coalesce(active_to::text,'2099-01-01'), 'YYYY-MM-DD') as active_to
    
    from targets

)

select * from cleaned