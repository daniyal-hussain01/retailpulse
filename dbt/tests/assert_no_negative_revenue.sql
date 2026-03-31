-- dbt/tests/assert_no_negative_revenue.sql
-- Custom singular test: fails if any order has negative net revenue.
-- Run with: dbt test --select assert_no_negative_revenue

select
    order_id,
    net_revenue_cents
from {{ ref('fact_orders') }}
where net_revenue_cents < 0

-- A non-empty result = test failure
