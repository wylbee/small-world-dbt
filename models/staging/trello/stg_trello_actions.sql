with 

raw_data as (

    select * from {{ source('raw_trello', 'actions') }}

),

cleaned as (

    select 
        id as action_id,
        data__board__id as board_id,
        data__list__id as list_id,
        "data__listAfter__id" as to_list_id,
        "data__listBefore__id" as from_list_id,
        data__card__id as card_id,

        data__board__name as board_name,
        data__list__name as list_name,
        "data__listAfter__name" as to_list_name,
        "data__listBefore__name" as from_list_name,
        data__card__name as card_name,
        type as action_type,

        data__card__closed as is_archived,

        to_timestamp(date, 'YYYY-MM-DD HH24:MI:SS') as action_date,

        _sdc_batched_at as el_batched_at,
        _sdc_received_at as el_recieved_at,
        _sdc_sequence as el_sequence
    
    from raw_data

),

-- data only from Personal Kanban board after system refactored to current
filtered as (

    select * from cleaned 

    where 
        board_id = '5b742ee2bdf40b08536e560e'

)

select * from filtered