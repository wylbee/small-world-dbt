{{ config(materialized='table') }}


{{ dbt_utils.date_spine(
    datepart="hour",
    start_date="to_date('06/01/2019', 'mm/dd/yyyy')",
    end_date="current_date + interval '1 day'"
   )
}}