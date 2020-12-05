with 

base as (

    select * from {{ ref('stg_toggl') }}

),

people as (

    select * from {{ ref('dim_people') }}

),

tasks as (

    select * from {{ ref('dim_tasks') }}

),

fact as (

    select
        tracker_id,
        people_id,
        task_id, 
        date_started,
        date_ended,
        duration_seconds

    from base

    left outer join people 
        on base.toggl_user_id = people.toggl_user_id
    
    left outer join tasks 
        on
            base.client_name = tasks.client_name and 
            base.project_name = tasks.project_name and 
            base.task_description = tasks.task_description and
            base.context_tags = tasks.context_tags

)

select * from fact