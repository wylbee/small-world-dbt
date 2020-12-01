with

time as (

    select * from {{ ref('stg_toggl') }}

),

stitched as (

    select distinct
        {{ dbt_utils.surrogate_key(
            [
                'toggl_user_id'
            ]
        ) }} as people_id,
        toggl_user_id,
        user_name as full_name
    
    from time

)

select * from stitched