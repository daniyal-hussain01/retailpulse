{% snapshot snap_users %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id_hash',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

/*
  SCD Type 2 snapshot of the users dimension.

  Because user_id is PII-hashed in the Silver layer, this snapshot
  operates on user_id_hash as the unique key. Each change to a user record
  (e.g. acquisition_channel updated, country_code changed) creates a new
  version row with dbt_valid_from / dbt_valid_to populated automatically.

  Use case: accurate historical attribution — "what channel did this user
  come from at the time of their first order?" requires point-in-time lookup.

  In production, source this from a Silver users parquet table.
  For local dev with DuckDB, we build from the seed data directly.
*/

select
    sha256(concat('{{ var("pii_salt", "dev_salt") }}', '|', user_id))   as user_id_hash,
    country_code,
    acquisition_channel,
    created_at,
    updated_at

from read_parquet('{{ var("silver_path") }}/users/*.parquet')

{% endsnapshot %}
