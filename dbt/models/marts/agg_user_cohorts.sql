-- dbt/models/marts/agg_user_cohorts.sql
-- Monthly acquisition cohort retention analysis.
-- For each acquisition cohort (month of first order), shows how many users
-- placed orders in subsequent months. Powers the retention heatmap dashboard.

{{
  config(
    materialized='table',
    description='Monthly cohort retention. One row per (cohort_month, period_offset).'
  )
}}

with first_orders as (

    select
        user_id_hash,
        date_trunc('month', min(order_date))   as cohort_month,
        min(order_date)                         as first_order_date
    from {{ ref('fact_orders') }}
    where not is_refunded_or_cancelled
      and user_id_hash != 'ERASED'
    group by user_id_hash

),

order_months as (

    select distinct
        f.user_id_hash,
        fo.cohort_month,
        date_trunc('month', f.order_date)       as activity_month
    from {{ ref('fact_orders') }} f
    inner join first_orders fo on f.user_id_hash = fo.user_id_hash
    where not f.is_refunded_or_cancelled
      and f.user_id_hash != 'ERASED'

),

cohort_sizes as (

    select
        cohort_month,
        count(distinct user_id_hash)             as cohort_size
    from first_orders
    group by cohort_month

),

retention as (

    select
        om.cohort_month,
        om.activity_month,
        date_diff('month', om.cohort_month, om.activity_month)  as period_offset,
        count(distinct om.user_id_hash)          as retained_users

    from order_months om
    group by 1, 2, 3

)

select
    r.cohort_month::varchar                      as cohort_month,
    r.period_offset,
    r.activity_month::varchar                    as activity_month,
    cs.cohort_size,
    r.retained_users,
    {{ safe_divide('r.retained_users', 'cs.cohort_size') }} * 100   as retention_rate_pct,
    current_timestamp                            as dbt_loaded_at

from retention r
inner join cohort_sizes cs on r.cohort_month = cs.cohort_month

order by r.cohort_month, r.period_offset
