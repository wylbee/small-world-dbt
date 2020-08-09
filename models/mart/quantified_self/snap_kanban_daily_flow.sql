{{ config(materialized='table') }}

with

hourly_status as (

    select * from {{ ref('kanban_column_status_by_hour') }}

),

spine as (

    select * from {{ ref('day_date_spine') }}

),

select_records as (

    select 
        *,

        row_number() over (
            partition by card_id
            order by 
                hierarchy asc nulls last,
                date_hour
        ) = 1 as is_arrival_record,

        row_number() over (
            partition by 
                card_id,
                date_hour::timestamp::date
            order by
                date_hour asc nulls last
        ) = 1 as is_first_record_of_day
    
    from hourly_status

),

joined as (

    select 
        spine.date_day,

        count(
            case 
                when select_records.is_arrival_record then   select_records.card_id
            end 
        ) as num_arrivals,
        
        count(
            case 
                when 
                    select_records.is_first_record_of_day and 
                    select_records.column_name not in ('Archived', 'Done')
                    then select_records.card_id
            end
        ) as num_inventory
    
    from spine

    left outer join select_records
        on spine.date_day = select_records.date_hour::timestamp::date

    group by 1

)

select * from joined