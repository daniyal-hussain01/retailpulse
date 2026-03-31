-- dbt/models/staging/stg_orders.sql
-- Reads from Silver parquet files and standardises column names/types.

{{
  config(
    materialized='view',
    description='Cleaned order records from Silver layer. PII is pre-hashed upstream.'
  )
}}

with source as (

    select *
    from read_parquet('{{ var("silver_path") }}/orders/*.parquet')

),

renamed as (

    select
        order_id                                        as order_id,
        user_id_hash                                    as user_id_hash,
        warehouse_id                                    as warehouse_id,
        status                                          as order_status,
        currency                                        as currency,
        cast(subtotal_cents as bigint)                  as subtotal_cents,
        cast(discount_cents as bigint)                  as discount_cents,
        cast(shipping_cents as bigint)                  as shipping_cents,
        cast(total_cents    as bigint)                  as total_cents,
        cast(total_cents - discount_cents as bigint)    as net_revenue_cents,
        cdc_op,
        cast(cdc_ts_ms as bigint)                       as cdc_ts_ms,
        cast(created_at as timestamp)                   as created_at,
        cast(updated_at as timestamp)                   as updated_at,
        date_trunc('day', cast(created_at as timestamp)) as order_date

    from source

    where order_id is not null
      and user_id_hash is not null
      and total_cents > 0

)

select * from renamed
