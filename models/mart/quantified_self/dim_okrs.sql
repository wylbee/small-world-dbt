with

objectives as (

    select * from {{ ref('stg_okr_objectives') }}

),

key_results as (

    select * from {{ ref('stg_okr_key_results') }}

),

joined as (

    select 
        objectives.objective_id,
        objectives.objective_category_id,
        key_results.key_result_id,
        
        objectives.objective_text,
        key_results.key_result_text,

        key_results.key_result_value,
        key_results.key_result_unit,
        key_results.key_result_metric_name,

        objectives.date_active_from as date_active_from_objective,
        objectives.date_active_to as date_active_to_objective,
        objectives.date_objective_target,
        key_results.date_active_from as date_active_from_key_result,
        key_results.date_active_to as date_active_to_key_result,
        key_results.date_key_result_target

    from key_results

    left outer join objectives 
        on key_results.objective_id = objectives.objective_id

)

select * from joined