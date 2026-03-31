-- scripts/sql/02_seed.sql
-- Generates realistic seed data for local development.

INSERT INTO warehouses (warehouse_id, name, city, state_code, country_code, zip_code, latitude, longitude) VALUES
  ('a1b2c3d4-0001-0001-0001-000000000001', 'East Coast Hub',   'Newark',      'NJ', 'US', '07102', 40.7357, -74.1724),
  ('a1b2c3d4-0002-0002-0002-000000000002', 'West Coast Hub',   'Los Angeles', 'CA', 'US', '90001', 33.9731, -118.2479),
  ('a1b2c3d4-0003-0003-0003-000000000003', 'Central Hub',      'Chicago',     'IL', 'US', '60601', 41.8827, -87.6233)
ON CONFLICT DO NOTHING;

INSERT INTO products (product_id, sku, name, category, brand, cost_price_cents, retail_price_cents) VALUES
  ('b1b2c3d4-0001-0001-0001-000000000001', 'SKU-SHIRT-BLU-M', 'Blue Oxford Shirt M',     'Apparel',     'UrbanThread', 1200, 4999),
  ('b1b2c3d4-0002-0002-0002-000000000002', 'SKU-SHIRT-WHT-L', 'White Oxford Shirt L',    'Apparel',     'UrbanThread', 1200, 4999),
  ('b1b2c3d4-0003-0003-0003-000000000003', 'SKU-SHOE-RUN-10', 'Running Shoe Size 10',    'Footwear',    'SpeedStep',   2800, 8999),
  ('b1b2c3d4-0004-0004-0004-000000000004', 'SKU-BAG-TOTE-BLK','Black Tote Bag',          'Accessories', 'CarryAll',    800,  2999),
  ('b1b2c3d4-0005-0005-0005-000000000005', 'SKU-HAT-CAP-RED', 'Red Baseball Cap',        'Accessories', 'CapCo',       400,  1499),
  ('b1b2c3d4-0006-0006-0006-000000000006', 'SKU-JEAN-SLM-32', 'Slim Jeans W32',          'Apparel',     'DenimLab',    2200, 6999),
  ('b1b2c3d4-0007-0007-0007-000000000007', 'SKU-SOCK-WHT-ML', 'White Sport Socks 3-Pack','Apparel',     'ComfortStep', 300,  999),
  ('b1b2c3d4-0008-0008-0008-000000000008', 'SKU-JACKET-BLK-M','Black Bomber Jacket M',   'Outerwear',   'NorthLayer',  5500, 14999)
ON CONFLICT DO NOTHING;

INSERT INTO inventory (product_id, warehouse_id, quantity_on_hand, reorder_point)
SELECT p.product_id, w.warehouse_id,
       (random() * 200 + 10)::int,
       15
FROM products p CROSS JOIN warehouses w
ON CONFLICT DO NOTHING;

INSERT INTO users (user_id, email, first_name, last_name, country_code, acquisition_channel, created_at) VALUES
  ('c1b2c3d4-0001-0001-0001-000000000001', 'alice@example.com',  'Alice',  'Chen',     'US', 'organic',     now() - interval '180 days'),
  ('c1b2c3d4-0002-0002-0002-000000000002', 'bob@example.com',    'Bob',    'Martinez', 'US', 'paid_search', now() - interval '120 days'),
  ('c1b2c3d4-0003-0003-0003-000000000003', 'carol@example.com',  'Carol',  'Kim',      'CA', 'email',       now() - interval '90 days'),
  ('c1b2c3d4-0004-0004-0004-000000000004', 'dave@example.com',   'Dave',   'Patel',    'GB', 'referral',    now() - interval '60 days'),
  ('c1b2c3d4-0005-0005-0005-000000000005', 'emma@example.com',   'Emma',   'Wilson',   'US', 'paid_social', now() - interval '30 days')
ON CONFLICT DO NOTHING;

-- Insert 20 orders spread over the past 30 days
DO $$
DECLARE
  user_ids UUID[] := ARRAY[
    'c1b2c3d4-0001-0001-0001-000000000001'::UUID,
    'c1b2c3d4-0002-0002-0002-000000000002'::UUID,
    'c1b2c3d4-0003-0003-0003-000000000003'::UUID,
    'c1b2c3d4-0004-0004-0004-000000000004'::UUID,
    'c1b2c3d4-0005-0005-0005-000000000005'::UUID
  ];
  product_ids UUID[] := ARRAY[
    'b1b2c3d4-0001-0001-0001-000000000001'::UUID,
    'b1b2c3d4-0002-0002-0002-000000000002'::UUID,
    'b1b2c3d4-0003-0003-0003-000000000003'::UUID,
    'b1b2c3d4-0004-0004-0004-000000000004'::UUID,
    'b1b2c3d4-0005-0005-0005-000000000005'::UUID,
    'b1b2c3d4-0006-0006-0006-000000000006'::UUID,
    'b1b2c3d4-0007-0007-0007-000000000007'::UUID,
    'b1b2c3d4-0008-0008-0008-000000000008'::UUID
  ];
  wh_ids UUID[] := ARRAY[
    'a1b2c3d4-0001-0001-0001-000000000001'::UUID,
    'a1b2c3d4-0002-0002-0002-000000000002'::UUID,
    'a1b2c3d4-0003-0003-0003-000000000003'::UUID
  ];
  statuses TEXT[] := ARRAY['placed','processing','shipped','delivered','delivered','delivered'];
  oid UUID;
  pid UUID;
  qty INT;
  unit_price INT;
  total INT;
BEGIN
  FOR i IN 1..20 LOOP
    oid := gen_random_uuid();
    pid := product_ids[1 + (random()*7)::int % 8];
    qty := 1 + (random()*3)::int;

    SELECT retail_price_cents INTO unit_price
    FROM products WHERE product_id = pid;

    total := unit_price * qty;

    INSERT INTO orders (order_id, user_id, warehouse_id, status, subtotal_cents, total_cents, created_at)
    VALUES (
      oid,
      user_ids[1 + (random()*4)::int % 5],
      wh_ids[1 + (random()*2)::int % 3],
      statuses[1 + (random()*5)::int % 6],
      total,
      total,
      now() - (random() * interval '30 days')
    );

    INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
    VALUES (oid, pid, qty, unit_price);
  END LOOP;
END;
$$;

-- Insert sample clickstream events
INSERT INTO events (session_id, user_id, event_type, product_id, page_url, occurred_at)
SELECT
  'sess-' || floor(random() * 100)::text,
  (ARRAY[
    'c1b2c3d4-0001-0001-0001-000000000001'::UUID,
    'c1b2c3d4-0002-0002-0002-000000000002'::UUID,
    NULL
  ])[1 + (random()*2)::int % 3],
  (ARRAY['page_view','product_view','add_to_cart','search'])[1 + (random()*3)::int % 4],
  (ARRAY[
    'b1b2c3d4-0001-0001-0001-000000000001'::UUID,
    'b1b2c3d4-0002-0002-0002-000000000002'::UUID,
    'b1b2c3d4-0003-0003-0003-000000000003'::UUID,
    'b1b2c3d4-0004-0004-0004-000000000004'::UUID,
    'b1b2c3d4-0005-0005-0005-000000000005'::UUID,
    'b1b2c3d4-0006-0006-0006-000000000006'::UUID,
    'b1b2c3d4-0007-0007-0007-000000000007'::UUID,
    'b1b2c3d4-0008-0008-0008-000000000008'::UUID
  ])[1 + (random()*7)::int % 8],
  '/products/item-' || floor(random() * 100)::text,
  now() - (random() * interval '7 days')
FROM generate_series(1, 200);
