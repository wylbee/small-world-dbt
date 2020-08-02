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

        coalesce(column_mapping.cleaned, actions.list_name) as column_name,

        actions.action_date

    from actions 

    left outer join column_mapping
        on actions.list_name = column_mapping.actual

    where 
        actions.list_id is not null and 
        actions.board_name = 'Personal Kanban'

)

select * from add_columns