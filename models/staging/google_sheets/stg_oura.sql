with 

raw as (

    select * from {{ source('raw','oura_api') }}

),

cleaned as (

    select 
        {{ dbt_utils.surrogate_key(
            [
                'summary_date',
                'category',
                'metric_name'
            ]
        ) }} as oura_id,
        summary_date,
        category,
        metric_name,
        value
    
    from raw

)

select * from cleaned