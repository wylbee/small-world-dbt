with

raw_data as (

    select * from {{ source('raw', 'toggl_api') }}

),

cleaned as (

    select
        {{ dbt_utils.surrogate_key(
            [
                '"id"'
            ]
        ) }} as tracker_id,

        "id" as toggl_record_id,
        "pid" as toggl_project_id,
        "uid" as toggl_user_id,

        "client" as client_name,
        "project" as project_name,
        "user" as user_name,
        "description" as task_description,
        to_timestamp("start", 'YYYY-MM-DD hh24:mi:ss') :: timestamptz as date_started,
        to_timestamp("end", 'YYYY-MM-DD hh24:mi:ss') :: timestamptz as date_ended,
        to_timestamp("updated", 'YYYY-MM-DD hh24:mi:ss') :: timestamptz as date_last_updated,
        "dur"/1000 :: int as duration_seconds,
        to_json("tags"::text) #>> '{}' as context_tags
    
    from raw_data

)

select * from cleaned