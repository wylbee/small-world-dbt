with 

zet as (

    select * from {{ ref('zettelkasten_snapshot') }}

),

cleaned as (

    select
        {{ dbt_utils.surrogate_key(
            [
                'atom_id',
                'dbt_scd_id'
            ]
        ) }} as atom_snapshot_record_id,

        atom_id,
        title as atom_title,
        status as atom_status,
        to_timestamp(atom_id,'YYYYMMDDHH24MISS') as atom_created_date,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    
    from zet

),

historical_override as (

    select
        *,

        case
            when dbt_updated_at < '2020-12-10' then atom_created_date
            else dbt_updated_at
        end as override_dbt_updated_at
    
    from cleaned

)

select * from historical_override