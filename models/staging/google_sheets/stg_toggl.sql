with

raw_data as (

    select * from {{ source('raw', 'toggl') }}

),

cleaned as (

    select
        {{ dbt_utils.surrogate_key(
            [
                '"Start date"',
                '"Start time"',
                '"End date"',
                '"End time"'
            ]
        ) }} as tracker_id,

        "Client" as client_name,
        "Project" as project_name,
        "Description" as task_description,
        "Start date" as date_started,
        "Start time" as time_started,
        "End date" as date_ended,
        "End time" as time_ended,
        "Duration" as duration,
        "Tags" as context_tags
    
    from raw_data

)

select * from cleaned