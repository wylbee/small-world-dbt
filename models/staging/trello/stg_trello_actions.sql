with 

raw_data as (

    select * from {{ source('raw_trello', 'actions') }}

),

cleaned as (

    select 
        id as action_id
    
    from raw_data

)

select * from cleaned