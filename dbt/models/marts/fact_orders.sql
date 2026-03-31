-- dbt/models/marts/fact_orders.sql
-- Core fact table: one row per order.
-- Joins to dim tables for warehouse and date keys.

{{
  config(
    materialized='table',
    description='Gold layer fact table. One row per order. All amounts in cents (USD).'
  )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

),

-- Order-level aggregation from items (when available)
order_summary as (

    select
        order_id,
        count(*) over (partition by order_id)   as item_count

    from orders

),

final as (

    select
        o.order_id,
        o.user_id_hash,
        o.warehouse_id,

        -- Status flags
        o.order_status,
        o.order_status = 'delivered'                    as is_delivered,
        o.order_status in ('refunded', 'cancelled')     as is_refunded_or_cancelled,

        -- Amounts
        o.currency,
        o.subtotal_cents,
        o.discount_cents,
        o.shipping_cents,
        o.total_cents,
        o.net_revenue_cents,

        -- Date dimensions
        o.order_date,
        date_part('year',  o.created_at)::int  as order_year,
        date_part('month', o.created_at)::int  as order_month,
        date_part('dow',   o.created_at)::int  as order_day_of_week,

        -- Metadata
        o.cdc_op,
        o.created_at,
        o.updated_at,
        current_timestamp                       as dbt_loaded_at

    from orders o

)

select * from final
