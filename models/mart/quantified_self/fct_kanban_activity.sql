{{ config(materialized='table') }}

with 

actions as (

    select * from {{ ref('stg_trello_actions')}}

),

column_mapping as (

    select * from {{ ref('personal_kanban_list_mapping') }}

),

add_columns as (

    select 
        actions.action_id,
        actions.card_id,

        actions.card_name,

        case
            when is_archived then 'Archived'
            else coalesce(column_mapping.cleaned, actions.to_list_name, actions.list_name) 
        end as column_name,

        actions.action_date

    from actions 

    left outer join column_mapping
        on coalesce(actions.list_name, actions.to_list_name) = column_mapping.actual

    where 
        (coalesce(actions.list_id, actions.to_list_id) is not null or is_archived) and 
        actions.board_name = 'Personal Kanban'

),

add_start_end as (

    select 
        *,

        action_date as start_date,

        lead(action_date, 1, current_timestamp) over (
            partition by card_id
            order by action_date
        ) as end_date
    
    from add_columns
)

select * from add_start_end