with

pc as (

    select * from {{ ref('stg_personal_capital_transactions') }}

),

--stubbed for m1
unioned as (

    select * from pc 

)

select * from unioned