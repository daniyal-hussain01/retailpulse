-- scripts/sql/01_schema.sql
-- Source OLTP schema for RetailPulse Postgres instance.
-- This runs automatically when the postgres-source container first starts.

SET client_min_messages = warning;

-- ── Users ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           TEXT NOT NULL UNIQUE,
    first_name      TEXT NOT NULL,
    last_name       TEXT NOT NULL,
    country_code    CHAR(2) NOT NULL DEFAULT 'US',
    acquisition_channel TEXT NOT NULL DEFAULT 'organic'
                         CHECK (acquisition_channel IN ('organic','paid_search','paid_social','email','referral')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Products ───────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
    product_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku             TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL,
    brand           TEXT,
    cost_price_cents INT NOT NULL CHECK (cost_price_cents >= 0),
    retail_price_cents INT NOT NULL CHECK (retail_price_cents >= 0),
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Warehouses ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS warehouses (
    warehouse_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    city            TEXT NOT NULL,
    state_code      CHAR(2),
    country_code    CHAR(2) NOT NULL DEFAULT 'US',
    zip_code        TEXT,
    latitude        NUMERIC(9,6),
    longitude       NUMERIC(9,6)
);

-- ── Inventory ──────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id      UUID NOT NULL REFERENCES products(product_id),
    warehouse_id    UUID NOT NULL REFERENCES warehouses(warehouse_id),
    quantity_on_hand INT NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    reorder_point   INT NOT NULL DEFAULT 10,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(product_id, warehouse_id)
);

-- ── Orders ─────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
    order_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id),
    warehouse_id    UUID REFERENCES warehouses(warehouse_id),
    status          TEXT NOT NULL DEFAULT 'placed'
                         CHECK (status IN ('placed','processing','shipped','delivered','refunded','cancelled')),
    currency        CHAR(3) NOT NULL DEFAULT 'USD',
    subtotal_cents  INT NOT NULL DEFAULT 0 CHECK (subtotal_cents >= 0),
    discount_cents  INT NOT NULL DEFAULT 0 CHECK (discount_cents >= 0),
    shipping_cents  INT NOT NULL DEFAULT 0,
    total_cents     INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Order items ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
    item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id        UUID NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id      UUID NOT NULL REFERENCES products(product_id),
    quantity        INT NOT NULL CHECK (quantity > 0),
    unit_price_cents INT NOT NULL CHECK (unit_price_cents >= 0),
    discount_cents  INT NOT NULL DEFAULT 0
);

-- ── Clickstream events (high-volume) ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id      TEXT NOT NULL,
    user_id         UUID REFERENCES users(user_id),  -- nullable (anonymous)
    event_type      TEXT NOT NULL
                         CHECK (event_type IN ('page_view','product_view','add_to_cart',
                                               'remove_from_cart','checkout_start',
                                               'purchase','search')),
    product_id      UUID REFERENCES products(product_id),
    page_url        TEXT,
    referrer_url    TEXT,
    search_query    TEXT,
    ip_address      INET,  -- PII — masked in Silver
    user_agent      TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── Indexes ────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_orders_user_id     ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_created_at  ON orders(created_at);
CREATE INDEX IF NOT EXISTS idx_order_items_order  ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_events_session     ON events(session_id);
CREATE INDEX IF NOT EXISTS idx_events_occurred_at ON events(occurred_at);
CREATE INDEX IF NOT EXISTS idx_inventory_product  ON inventory(product_id, warehouse_id);

-- ── Updated_at trigger ─────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY['users','products','orders','inventory']
  LOOP
    EXECUTE format(
      'DROP TRIGGER IF EXISTS trg_updated_at ON %I;
       CREATE TRIGGER trg_updated_at
       BEFORE UPDATE ON %I
       FOR EACH ROW EXECUTE FUNCTION set_updated_at();', t, t);
  END LOOP;
END;
$$;
