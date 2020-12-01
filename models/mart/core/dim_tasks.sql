with

time as (

    select * from {{ ref('stg_toggl') }}

),

stitched as (

    select distinct
        {{ dbt_utils.surrogate_key(
            [
                'client_name',
                'project_name',
                'task_description',
                'context_tags'
            ]
        ) }} as task_id,
        client_name,
        project_name,
        task_description,
        context_tags    

    from time

)

select * from stitched