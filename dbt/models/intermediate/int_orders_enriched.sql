-- dbt/models/intermediate/int_orders_enriched.sql
-- Enriches orders with computed business metrics.
-- Materialized as ephemeral (inline CTE in downstream models) to avoid
-- creating an intermediate table in the warehouse.

{{
  config(
    materialized='ephemeral',
    description='Orders with derived business fields. Feeds fact_orders and mart aggregations.'
  )
}}

with base as (

    select * from {{ ref('stg_orders') }}

),

enriched as (

    select
        order_id,
        user_id_hash,
        warehouse_id,
        order_status,
        currency,
        order_date,
        order_year,
        order_month,
        order_day_of_week,
        cdc_op,
        created_at,
        updated_at,

        -- Revenue fields
        subtotal_cents,
        discount_cents,
        shipping_cents,
        total_cents,
        net_revenue_cents,

        -- Computed flags
        order_status = 'delivered'                      as is_delivered,
        order_status in ('refunded', 'cancelled')       as is_refunded_or_cancelled,
        discount_cents > 0                              as has_discount,
        shipping_cents = 0                              as is_free_shipping,

        -- Revenue tier (for dashboard segmentation)
        case
            when total_cents >= 100_00  then 'high'    -- $100+
            when total_cents >= 30_00   then 'mid'     -- $30–$100
            else                             'low'     -- <$30
        end                                             as order_value_tier,

        -- Day-of-week label
        case order_day_of_week
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end                                             as order_day_name,

        -- Effective discount rate
        {{ safe_divide('discount_cents', 'total_cents') }} as discount_rate

    from base

)

select * from enriched
