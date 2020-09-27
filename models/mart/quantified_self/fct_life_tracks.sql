with 

tracks as (

    select * from {{ ref('stg_nomie') }}

),

books as (

    select * from {{ ref('stg_goodreads') }}
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

unioned as (

    select * from tracks

    union all 

    select * from books_cleaned

)

select * from unioned