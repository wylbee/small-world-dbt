with 

raw_data as (

    select * from {{ source('raw', 'goodreads') }}

),

cleaned as (

    select 
        "Book Id" as book_id,
        "Title" as book_title,
        "Author" as author_name,
        "Exclusive Shelf" as reading_status,

        to_date("Date Added", 'MM/DD/YYYY') as date_added,
        
        to_date("Date Read", 'MM/DD/YYYY') as date_read

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