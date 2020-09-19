with 

tracks as (

    select * from {{ ref('stg_nomie') }}

),

selected as (

    select
        *

    from tracks

)

select * from selected