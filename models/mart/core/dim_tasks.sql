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
        context_tags,

        case 
            when context_tags like '%deep-work%' then true
            else false
        end as is_deep_work,

        case 
            when 
                context_tags like '%professional%' or
                client_name = 'Year Up'
                then true
            else false
        end as is_professional,

        case 
            when context_tags like '%okr%' then true
            else false
        end as is_okr,

        case 
            when context_tags like '%slope-learning%' then true
            else false
        end as is_slope_learning

    from time

)

select * from stitched