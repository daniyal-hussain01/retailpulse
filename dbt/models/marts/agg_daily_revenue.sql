-- dbt/models/marts/agg_daily_revenue.sql
-- Daily revenue rollup — powers the executive KPI dashboard.

{{
  config(
    materialized='table',
    description='Daily revenue aggregated from fact_orders. Refreshed nightly.'
  )
}}

select
    order_date,
    order_year,
    order_month,

    count(distinct order_id)                                    as order_count,
    count(distinct user_id_hash)                                as unique_buyers,

    sum(total_cents)                                            as gross_revenue_cents,
    sum(net_revenue_cents)                                      as net_revenue_cents,
    sum(discount_cents)                                         as total_discount_cents,
    sum(shipping_cents)                                         as total_shipping_cents,

    -- Averages
    avg(total_cents)::bigint                                    as avg_order_value_cents,

    -- Refund metrics
    count_if(is_refunded_or_cancelled)                          as refunded_order_count,
    round(
        100.0 * count_if(is_refunded_or_cancelled)
        / nullif(count(*), 0),
        2
    )                                                           as refund_rate_pct,

    -- Fulfilment
    count_if(is_delivered)                                      as delivered_order_count,

    current_timestamp                                           as dbt_loaded_at

from {{ ref('fact_orders') }}

group by 1, 2, 3
order by 1 desc
