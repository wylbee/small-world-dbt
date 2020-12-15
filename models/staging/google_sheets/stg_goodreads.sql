with 

raw_csv as (

    select * from {{ source('raw', 'goodreads') }}

),

raw_api as (

    select * from {{ source('raw', 'goodreads_api') }}

),

cleaned_csv as (

    select 
        "Title" as book_title,
        "Author" as author_name,
        "Exclusive Shelf" as reading_status,

        to_date("Date Added", 'YYYY/MM/DD') as date_added,
        
        to_date("Date Read", 'YYYY/MM/DD') as date_read

    from raw_csv

),

cleaned_api as (

    select 
        title as book_title,
        author as author_name,
        
        case 
            when shelves = '' then 'read'
            else shelves
        end as reading_status,

        to_date(date_added, 'YYYY/MM/DD') as date_added,

        to_date(read_at, 'YYYY/MM/DD') as date_read

    from raw_api

),

anti_join as (

    select cleaned_csv.* from cleaned_csv

    left outer join cleaned_api
        on 
            cleaned_csv.book_title = cleaned_api.book_title and
            cleaned_csv.author_name = cleaned_api.author_name
    
    where 
        cleaned_api.book_title is null and 
        cleaned_api.author_name is null

),

unioned as (

    select * from cleaned_api

    union 

    select * from anti_join

),

add_id as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'book_title',
                'author_name'
            ]
        ) }} as book_id,
        *
    
    from unioned

)

select * from add_id