with 

tracks as (

    select * from {{ ref('stg_nomie') }}

),

books as (

    select * from {{ ref('stg_goodreads') }}

),

toggl as (

    select * from {{ ref('stg_toggl') }}

),

books_cleaned as (

    select 
        tracker_id,
        'book_read' as tracker_name,
        'general' as tracker_context,
        book_title as tracker_note,

        1 as tracker_value,

        'books' as tracker_units,
        date_read as date_completed
    
    from books

    where reading_status = 'read'

),

toggl_cleaned as (

    select
        tracker_id,

        case 
            when context_tags like '%deep-work%' then 'deep_work'
            when context_tags like '%slope-learning%' then 'slope_learning'
        end as tracker_name,

        case
            when context_tags like '%professional%' then 'professional'
            when context_tags like '%craft%' then 'craft'
            when 
                context_tags like '%slope-learning%' and 
                project_name like '%Machine Learning Engineering%'
                then 'aml'
            else 'general'
        end as tracker_context,

        task_description as tracker_note,

        duration_seconds as tracker_value,

        'seconds' as tracker_units,
        date_ended as date_completed
    
    from toggl
    
    where 
        context_tags like '%deep-work%' or 
        context_tags like '%slope-learning%'

),

unioned as (

    select * from tracks

    union all 

    select * from books_cleaned

    union all 

    select * from toggl_cleaned

)

select * from unioned