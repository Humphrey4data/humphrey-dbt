{% snapshot scorecard_qa_snapshot_stg %}

{{
    config(
      target_database='pai_prod_dw',
      target_schema='main',
      unique_key='KEY',

      strategy='timestamp',
      updated_at='last_update',
    )
}}

select * from {{ ref('SCORECARD_QA') }}

{% endsnapshot %}