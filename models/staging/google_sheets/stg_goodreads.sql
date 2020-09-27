with 

raw_data as (

    select * from {{ source('raw_goodreads', 'export') }}

),

cleaned as (

    select 
        "Book Id" as book_id,
        "Title" as book_title,
        "Author" as author_name,
        "Exclusive Shelf" as reading_status,

        to_date("Date Added", 'YYYY-MM-DD') as date_added,
        
        date '1899-12-30' + "Date Read"::int * interval '1' day as date_read

    from raw_data

),

add_id as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'book_id'
            ]
        ) }} as tracker_id,
        *
    
    from cleaned 

)

select * from add_id