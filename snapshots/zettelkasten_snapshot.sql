{{ config(schema='snapshot') }}

{% snapshot zettelkasten_snapshot %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='atom_id',
          check_cols=['title', 'status'],
        )
    }}

    select * from {{ source('raw', 'zettelkasten') }}

{% endsnapshot %}